package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"github.com/temoto/http-client/heroshi" // Temporary location
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var urls chan *url.URL
var reports chan []byte

func handleSigInt(ch <-chan os.Signal) {
	defer close(urls)

	<-ch
	log.Println("Waiting for remaining requests to complete.")
}

func stdinReader() {
	defer close(urls)

	var line string
	var u *url.URL
	var err error
	stdinReader := bufio.NewReader(os.Stdin)
	for {
		lineBytes, readErr := stdinReader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			panic("At ReadBytes")
			return
		}

		lineBytes = bytes.TrimSpace(lineBytes)
		if len(lineBytes) == 0 {
			goto Next
		}
		line = string(lineBytes)

		u, err = url.Parse(line)
		if err != nil {
			u = &url.URL{
				Host: line,
			}
			result := heroshi.ErrorResult(u, err.Error())
			reportJson, _ := encodeResult(line, result)
			reports <- reportJson
		} else {
			urls <- u
		}

	Next:
		if readErr == io.EOF {
			return
		}
	}
}

func encodeResult(key string, result *heroshi.FetchResult) (encoded []byte, err error) {
	// Copy of FetchResult struct with new field Key and base64-encoded Body.
	// This is ugly and violates DRY principle.
	// But also, it allows to extract fetcher as separate package.
	var report struct {
		Key        string              `json:"key"`
		Url        string              `json:"url"`
		Success    bool                `json:"success"`
		Status     string              `json:"status"`
		StatusCode int                 `json:"status_code"`
		Headers    map[string][]string `json:"headers,omitempty"`
		Content    string              `json:"content,omitempty"`
		Length     int64               `json:"length,omitempty"`
		Cached     bool                `json:"cached"`
		FetchTime  uint                `json:"fetch_time,omitempty"`
		TotalTime  uint                `json:"total_time,omitempty"`
		// new
		RemoteAddr     string `json:"address"`
		Started        string `json:"started"`
		ConnectionAge  uint   `json:"connection_age"`
		ConnectionUse  uint   `json:"connection_use"`
		ConnectTime    uint   `json:"connect_time"`
		WriteTime      uint   `json:"write_time,omitempty"`
		ReadHeaderTime uint   `json:"read_header_time,omitempty"`
		ReadBodyTime   uint   `json:"read_body_time,omitempty"`
	}
	report.Key = key
	report.Url = result.Url.String()
	report.Success = result.Success
	report.Status = result.Status
	report.StatusCode = result.StatusCode
	report.Headers = result.Headers
	report.Cached = result.Cached
	report.FetchTime = result.FetchTime
	report.TotalTime = result.TotalTime
	content_encoded := make([]byte, base64.StdEncoding.EncodedLen(len(result.Body)))
	base64.StdEncoding.Encode(content_encoded, result.Body)
	report.Content = string(content_encoded)
	report.Length = result.Length
	// new
	if result.Stat != nil {
		report.RemoteAddr = result.Stat.RemoteAddr.String()
		report.Started = result.Stat.Started.UTC().Format(time.RFC3339)
		report.ConnectionAge = uint(result.Stat.ConnectionAge / time.Millisecond)
		report.ConnectionUse = result.Stat.ConnectionUse
		report.ConnectTime = uint(result.Stat.ConnectTime / time.Millisecond)
		report.WriteTime = uint(result.Stat.WriteTime / time.Millisecond)
		report.ReadHeaderTime = uint(result.Stat.ReadHeaderTime / time.Millisecond)
		report.ReadBodyTime = uint(result.Stat.ReadBodyTime / time.Millisecond)
	}

	encoded, err = json.Marshal(report)
	if err != nil {
		encoded = nil
		log.Printf("Url: %s, error encoding report: %s\n",
			result.Url, err.Error())

		// Most encoding errors happen in content. Try to recover.
		report.Content = ""
		report.Status = err.Error()
		report.Success = false
		report.StatusCode = 0
		encoded, err = json.Marshal(report)
		if err != nil {
			encoded = nil
			log.Printf("Url: %s, error encoding recovery report: %s\n",
				result.Url, err.Error())
		}
	}
	return
}

func reportWriter(done chan bool) {
	for r := range reports {
		if r != nil {
			os.Stdout.Write(r)
			os.Stdout.Write([]byte("\n"))
		}
	}
	done <- true
}

func main() {
	worker := newWorker()
	urls = make(chan *url.URL)

	// Process command line arguments.
	var max_concurrency uint
	flag.UintVar(&max_concurrency, "jobs", 1000, "Try to crawl this many URLs in parallel.")
	flag.UintVar(&worker.FollowRedirects, "redirects", 10, "How many redirects to follow. Can be 0.")
	flag.UintVar(&worker.KeepAlive, "keepalive", 120, "Keep persistent connections to servers for this many seconds.")
	flag.BoolVar(&worker.SkipRobots, "skip-robots", false, "Don't request and obey robots.txt.")
	flag.BoolVar(&worker.SkipBody, "skip-body", false, "Don't return response body in results.")
	flag.DurationVar(&worker.ConnectTimeout, "connect-timeout", 15*time.Second, "Timeout to query DNS and establish TCP connection.")
	flag.DurationVar(&worker.IOTimeout, "io-timeout", 30*time.Second, "Timeout for sending request and receiving response (applied for each, so total time is twice this timeout).")
	flag.DurationVar(&worker.FetchTimeout, "total-timeout", 60*time.Second, "Total timeout for crawling one URL. Includes all network IO, fetching and checking robots.txt.")
	show_help := flag.Bool("help", false, "")
	flag.Parse()
	if max_concurrency <= 0 {
		log.Println("Invalid concurrency limit:", max_concurrency)
		os.Exit(1)
	}
	if *show_help {
		os.Stderr.WriteString(`HTTP client.
Reads URLs on stdin, fetches them and writes results as JSON on stdout.

Follows up to 10 redirects.
Fetches /robots.txt first and obeys rules there using User-Agent "Bot/0.4".

Try 'echo http://localhost/ |http-client' to see sample of result JSON.
`)
		os.Exit(1)
	}

	reports = make(chan []byte, max_concurrency)
	done_writing := make(chan bool)

	sig_int_chan := make(chan os.Signal, 1)
	signal.Notify(sig_int_chan, syscall.SIGINT)
	go handleSigInt(sig_int_chan)

	go stdinReader()
	go reportWriter(done_writing)

	limit := make(chan bool, max_concurrency)
	busy_count := make(chan uint, 1)
	url_count := 0

	busyCountGet := func() uint {
		n := <-busy_count
		busy_count <- n
		return n
	}

	processUrl := func(url *url.URL) {
		result := worker.Fetch(url)
		report_json, _ := encodeResult(url.String(), result)

		// nil report is really unrecoverable error. Check stderr.
		if report_json != nil {
			reports <- report_json
		}

		busy_count <- (<-busy_count - 1) // atomic decrement
		<-limit
	}

	busy_count <- 0
	for url := range urls {
		limit <- true
		url_count++
		busy_count <- (<-busy_count + 1) // atomic increment
		go processUrl(url)

		if url_count%20 == 0 {
			// TODO:
			//println("--- URL #", url_count, "Worker has", len(worker.client.HttpClient.Transport), "clients.")
			println("--- URL #", url_count, "Worker has", -1, "clients.")
			runtime.GC()
		}
	}

	// Ugly poll until all urls are processed.
	for n := busyCountGet(); n > 0; n = busyCountGet() {
		time.Sleep(1 * time.Second)
		// TODO:
		//println("--- URL #", url_count, "Worker has", len(worker.clients), "clients.")
		println("--- URL #", url_count, "Worker has", -1, "clients.")
		runtime.GC()
	}
	close(reports)
	<-done_writing
}
