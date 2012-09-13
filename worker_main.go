package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const HeroshiTimeFormat = "2006-01-02T15:04:05"
const DefaultConcurrency = 1000

var urls chan *url.URL
var reports chan []byte

func handleSigInt(ch <-chan os.Signal) {
	defer close(urls)

	<-ch
	fmt.Fprintln(os.Stderr, "Waiting for remaining requests to complete.")
}

func stdinReader() {
	defer close(urls)

	var line_str string
	var line_url *url.URL
	var err error
	stdinReader := bufio.NewReader(os.Stdin)
	for {
		line, read_err := stdinReader.ReadBytes('\n')
		if read_err != nil && read_err != io.EOF {
			panic("At ReadBytes")
			return
		}

		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			goto Next
		}
		line_str = string(line)

		line_url, err = url.Parse(line_str)
		if err != nil {
			result := ErrorResult(line_str, err.Error())
			report_json, _ := encodeResult(line_str, result)
			reports <- report_json
		} else {
			urls <- line_url
		}

	Next:
		if read_err == io.EOF {
			return
		}
	}
}

func encodeResult(key string, result *FetchResult) (encoded []byte, err error) {
	// Copy of FetchResult struct with new field Key and base64-encoded Body.
	// This is ugly and violates DRY principle.
	// But also, it allows to extract fetcher as separate package.
	var report struct {
		Key        string              `json:"key"`
		Url        string              `json:"url"`
		Success    bool                `json:"success"`
		Status     string              `json:"status"`
		StatusCode int                 `json:"status_code"`
		Headers    map[string][]string `json:"headers"`
		Content    string              `json:"content"`
		Length     int64               `json:"length"`
		Cached     bool                `json:"cached"`
		Visited    string              `json:"visited"`
		FetchTime  uint                `json:"fetch_time"`
		TotalTime  uint                `json:"total_time"`
	}
	report.Key = key
	report.Url = result.Url
	report.Success = result.Success
	report.Status = result.Status
	report.StatusCode = result.StatusCode
	report.Headers = result.Headers
	report.Cached = result.Cached
	report.Visited = time.Now().UTC().Format(HeroshiTimeFormat)
	report.FetchTime = result.FetchTime
	report.TotalTime = result.TotalTime
	content_encoded := make([]byte, base64.StdEncoding.EncodedLen(len(result.Body)))
	base64.StdEncoding.Encode(content_encoded, result.Body)
	report.Content = string(content_encoded)
	report.Length = result.Length

	encoded, err = json.Marshal(report)
	if err != nil {
		encoded = nil
		fmt.Fprintf(os.Stderr, "Url: %s, error encoding report: %s\n",
			result.Url, err.Error())

		// Most encoding errors happen in content. Try to recover.
		report.Content = ""
		report.Status = err.Error()
		report.Success = false
		report.StatusCode = 0
		encoded, err = json.Marshal(report)
		if err != nil {
			encoded = nil
			fmt.Fprintf(os.Stderr, "Url: %s, error encoding recovery report: %s\n",
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
	flag.UintVar(&max_concurrency, "jobs", DefaultConcurrency, "Try to crawl this many URLs in parallel.")
	flag.UintVar(&worker.FollowRedirects, "redirects", 10, "How many redirects to follow. Can be 0.")
	flag.UintVar(&worker.KeepAlive, "keepalive", 120, "Keep persistent connections to servers for this many seconds.")
	flag.BoolVar(&worker.SkipRobots, "skip-robots", false, "Don't request and obey robots.txt.")
	flag.BoolVar(&worker.SkipBody, "skip-body", false, "Don't return response body in results.")
	flag.DurationVar(&worker.IOTimeout, "io-timeout", 30*time.Second, "Timeout for single socket operation (read, write).")
	flag.DurationVar(&worker.FetchTimeout, "total-timeout", 60*time.Second, "Total timeout for crawling one URL. Includes all network IO, fetching and checking robots.txt.")
	show_help := flag.Bool("help", false, "")
	flag.Parse()
	if max_concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "Invalid concurrency limit:", max_concurrency)
		os.Exit(1)
	}
	if *show_help {
		fmt.Fprint(os.Stderr, `Heroshi IO worker.
Reads URLs on stdin, fetches them and writes results as JSON on stdout.

By default, follows up to 10 redirects.
By default, fetches /robots.txt first and obeys rules there.

Try 'echo http://localhost/ | http-client' to see sample of result JSON.
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
