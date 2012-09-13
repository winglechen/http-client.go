package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"
)

// Docs say http.Client and http.Transport are concurrent-safe.
var http_client *http.Client = &http.Client{
	Jar: nil,
	Transport: &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConnsPerHost: 1,
	},
}

type Client struct {
	// Fetch lock. Prevents simultaneous `FetchWithTimeout` calls for proper
	// timeout accounting.
	// TODO: find a better way to implement it. Without a separate lock?
	fetch_lk sync.Mutex

	// Internal IO operations lock.
	lk sync.Mutex
}

type FetchOptions struct {
	// Timeout for single IO socket read or write.
	// Fetch consists of many IO operations. 30 seconds is reasonable, but
	// in some cases more could be required.
	// Default is 0 - no timeout.
	IOTimeout time.Duration

	TotalTimeout time.Duration
}

var DefaultOptions = &FetchOptions{
	IOTimeout: 30 * time.Second,
}

type FetchResult struct {
	Url        string
	Success    bool
	Status     string
	StatusCode int
	Headers    http.Header
	Body       []byte
	Length     int64
	Cached     bool
	FetchTime  uint
	TotalTime  uint
}

func ErrorResult(url, reason string) *FetchResult {
	return &FetchResult{
		Url:     url,
		Success: false,
		Status:  reason,
	}
}

// True if the specified HTTP status code is one for which the Get utility should
// automatically redirect.
func ShouldRedirect(statusCode int) bool {
	switch statusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect:
		//
		return true
	}
	return false
}

type dialerFunc func(net, addr string) (net.Conn, error)

func makeDialer(options *FetchOptions) dialerFunc {
	return func(netw, addr string) (net.Conn, error) {
		println("--- dial " + netw + " : " + addr)
		conn, err := net.DialTimeout(netw, addr, options.IOTimeout)
		if err != nil {
			return conn, err
		}
		tcp_conn := conn.(*net.TCPConn)
		tcp_conn.SetKeepAlive(true)
		tcp_conn.SetLinger(0)
		tcp_conn.SetNoDelay(true)
		return tcp_conn, err
	}
}

func (client *Client) Abort(req *http.Request) {
	panic("Not implemented")
}

func (client *Client) fetch(req *http.Request, options *FetchOptions) (result *FetchResult) {
	// debug
	if false {
		dump, _ := httputil.DumpRequest(req, true)
		print(string(dump))
	}

	resp, err := http_client.Do(req)
	if err != nil {
		return ErrorResult(req.URL.String(), err.Error())
	}

	var buf bytes.Buffer
	var body_len int64
	body_len, err = io.Copy(&buf, resp.Body)
	responseBody := buf.Bytes()
	if err != nil {
		return ErrorResult(req.URL.String(), err.Error())
	}
	resp.Body.Close()

	return &FetchResult{
		Url:        req.URL.String(),
		Success:    true,
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Body:       responseBody,
		Length:     body_len,
		Headers:    resp.Header,
	}
}

func (client *Client) Fetch(req *http.Request, options *FetchOptions) (result *FetchResult) {
	client.fetch_lk.Lock()
	defer client.fetch_lk.Unlock()

	if options == nil {
		options = DefaultOptions
	}

	transport := http_client.Transport.(*http.Transport)
	transport.Dial = makeDialer(options)

	rch := make(chan *FetchResult)
	started := time.Now()
	go func() {
		rch <- client.fetch(req, options)
	}()

	select {
	case result = <-rch:
	case <-time.After(options.TotalTimeout):
		client.Abort(req)
		result = ErrorResult(req.URL.String(), fmt.Sprintf("Fetch timeout: %d", options.TotalTimeout/time.Millisecond))
	}
	result.FetchTime = uint((time.Now().Sub(started)) / time.Millisecond)

	return result
}
