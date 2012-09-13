package main

import (
	"fmt"
	"github.com/temoto/robotstxt.go"
	"net/http"
	"net/url"
	"time"
	// Cache:
	//"github.com/hoisie/redis.go"
	//"json"
)

type Worker struct {
	// When false (default), worker will obey /robots.txt
	// when true, any URL is allowed to visit.
	SkipRobots bool

	// When false (default) worker will fetch and return response body
	// when true response body will be discarded after received.
	SkipBody bool

	// How many redirects to follow. Default is 1.
	FollowRedirects uint

	// Timeout for single socket read or write.
	// Default is 1 second. 0 disables timeout.
	IOTimeout time.Duration

	// Timeout for whole download. This includes establishing connection,
	// sending request, receiving response.
	// Default is 1 minute. DO NOT use 0.
	FetchTimeout time.Duration

	// How long to keep persistent connections, in seconds. Default is 60.
	KeepAlive uint

	// Maximum number of connections per domain:port pair. Default is 2.
	DomainConcurrency uint

	// HTTP client. Has pool of persistent connections to each host.
	client Client

	//cache redis.Client
}

func newWorker() *Worker {
	w := &Worker{
		FollowRedirects:   1,
		IOTimeout:         time.Duration(1) * time.Second,
		FetchTimeout:      time.Duration(60) * time.Second,
		KeepAlive:         60,
		DomainConcurrency: 2,
	}
	go w.cleanIdleConnections()
	return w
}

func (w *Worker) cleanIdleConnections() {
	time.Sleep(time.Duration(w.KeepAlive) * time.Second)
	//FIXME: w.client.HttpClient.Transport.CloseIdleConnections()
	go w.cleanIdleConnections()
}

// Downloads url and returns whatever result was.
// This function WILL NOT follow redirects.
func (w *Worker) Download(url *url.URL) (result *FetchResult) {
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return ErrorResult(url.String(), err.Error())
	}
	req.Header.Set("User-Agent", "HeroshiBot/0.3 (+http://temoto.github.com/heroshi/; temotor@gmail.com)")

	options := &FetchOptions{
		IOTimeout:    w.IOTimeout,
		TotalTimeout: w.FetchTimeout,
	}
	result = w.client.Fetch(req, options)
	// TODO: do not even fetch whole response body, just wait for its end
	if w.SkipBody {
		result.Body = nil
	}

	return result
}

/*
func (w *Worker) CacheOrDownload(url *url.URL) *FetchResult {
    key := url.String()

    if encoded, err := w.cache.Get(key); err == nil {
        cached := new(FetchResult)
        if err := json.Unmarshal(encoded, cached); err == nil {
            cached.Cached = true
            return cached
        }
    }
    result := w.Download(url)
    encoded, _ := json.Marshal(result)
    w.cache.Set(key, encoded)
    return result
}
*/

func (w *Worker) Fetch(url *url.URL) (result *FetchResult) {
	original_url := *url
	started := time.Now()

	for redirect := uint(0); redirect <= w.FollowRedirects; redirect++ {
		if url.Scheme == "" || url.Host == "" {
			result = ErrorResult(url.String(), "Incorrect URL: "+url.String())
			break
		}

		// The /robots.txt is always allowed, check others.
		if w.SkipRobots || url.Path == "/robots.txt" {
		} else {
			var allow bool
			allow, result = w.AskRobots(url)
			if !allow {
				break
			}
		}

		//result = w.CacheOrDownload(url)
		result = w.Download(url)
		if ShouldRedirect(result.StatusCode) {
			location := result.Headers.Get("Location")
			var err error
			url, err = url.Parse(location)
			if err != nil {
				result = ErrorResult(original_url.String(), err.Error())
				break
			}
			continue
		}

		// no redirects required
		break
	}
	ended := time.Now()
	result.TotalTime = uint((ended.Sub(started)) / 1e6) // in milliseconds
	return result
}

func (w *Worker) AskRobots(url *url.URL) (bool, *FetchResult) {
	robots_url_str := fmt.Sprintf("%s://%s/robots.txt", url.Scheme, url.Host)
	robots_url, err := url.Parse(robots_url_str)
	if err != nil {
		return false, ErrorResult(url.String(), err.Error())
	}

	fetch_result := w.Fetch(robots_url)

	if !fetch_result.Success {
		fetch_result.Status = "Robots download error: " + fetch_result.Status
		return false, fetch_result
	}

	var robots *robotstxt.RobotsData
	robots, err = robotstxt.FromResponseBytes(fetch_result.StatusCode, fetch_result.Body, false)
	if err != nil {
		fetch_result.Status = "Robots parse error: " + err.Error()
		return false, fetch_result
	}

	robots.DefaultAgent = "HeroshiBot"

	var allow bool
	allow, err = robots.Test(url.Path)
	if err != nil {
		return false, ErrorResult(url.String(), "Robots test error: "+err.Error())
	}

	if !allow {
		return allow, ErrorResult(url.String(), "Robots disallow")
	}

	return allow, nil
}
