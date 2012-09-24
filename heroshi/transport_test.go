package heroshi

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type ConnectionHandler func(*testing.T, net.Conn)

func fastServe(t *testing.T, conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReader(conn)
	_, err := http.ReadRequest(br)
	if err != nil {
		t.Error("fast:Read:", err.Error())
	}

	response := []byte("HTTP/1.0 200 OK\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: 100\r\n\r\n1xxxxxxxxx2xxxxxxxxx3xxxxxxxxx4xxxxxxxxx5xxxxxxxxx6xxxxxxxxx7xxxxxxxxx8xxxxxxxxx9xxxxxxxxxaxxxxxxxxx")
	n, err := conn.Write(response)
	if err != nil {
		t.Error("fast:Write:", err.Error())
	}
	if n < len(response) {
		t.Error("fast:Write: written", n, "bytes of", len(response))
	}
}

func slowServe(t *testing.T, conn net.Conn) {
	defer conn.Close()

	var request []byte
	buf := make([]byte, 4)
	for {
		time.Sleep(10 * time.Millisecond)
		n, err := conn.Read(buf[0:])
		if err != nil {
			t.Error("slow:Read:", err.Error())
			break
		}
		request = append(request, buf[0:n]...)
		if request[len(request)-2] == '\r' && request[len(request)-1] == '\n' {
			break
		}
	}

	time.Sleep(5 * time.Millisecond)
	conn.Write([]byte("HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 800\r\n\r\n"))
	for i := 1; i <= 100; i++ {
		time.Sleep(5 * time.Millisecond)
		conn.Write([]byte("response"))
	}
}

func delayServe(t *testing.T, conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReader(conn)
	_, err := http.ReadRequest(br)
	if err != nil {
		t.Error("delay:Read:", err.Error())
	}

	response := []byte("HTTP/1.0 200 OK\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: 19\r\n\r\nEverything is fine.")
	n, err := conn.Write(response)
	if err != nil {
		t.Error("delay:Write:", err.Error())
	}
	if n < len(response) {
		t.Error("fast:delay: written", n, "bytes of", len(response))
	}
}

func server(t *testing.T, listener net.Listener, connHandler ConnectionHandler, stopCh chan bool, acceptDelay time.Duration) {
	defer listener.Close()

	connCh := make(chan net.Conn, 0)
	errCh := make(chan error, 0)
	go func() {
		for {
			time.Sleep(acceptDelay)
			conn, err := listener.Accept()
			if err == nil {
				connCh <- conn
			} else {
				errCh <- err
			}
		}
	}()
AcceptLoop:
	for {
		select {
		case <-stopCh:
			break AcceptLoop
		case conn := <-connCh:
			go connHandler(t, conn)
		case err := <-errCh:
			if _, ok := <-stopCh; !ok {
				t.Error("Accept:", err.Error())
			} else {
				break AcceptLoop
			}
		}
	}
}

func TestConnectTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, fastServe, stopCh, 50*time.Millisecond)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/slow-connect", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	ready := make(chan bool, 0)
	go func() {
		transport := &Transport{}
		options := &RequestOptions{
			ConnectTimeout: 5 * time.Millisecond,
		}
		response, err := transport.RoundTripOptions(request, options)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				t.Log("Expected transport timeout:", neterr.Error())
			} else if err != nil {
				t.Error("Client error:", err.Error())
			}
		} else {
			response.Body.Close()
		}
		ready <- true
	}()
	select {
	case <-ready:
	case <-time.After(100 * time.Millisecond):
		t.Error("Transport did not timeout")
	}
}

func TestReadTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, slowServe, stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/slow-respond", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	ready := make(chan bool, 0)
	go func() {
		transport := &Transport{}
		options := &RequestOptions{
			ReadTimeout: 5 * time.Millisecond,
		}
		_, err = transport.RoundTripOptions(request, options)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				t.Log("Expected transport timeout:", neterr.Error())
				return
			} else if err != nil {
				t.Error("Client error:", err.Error())
			}
		}
		ready <- true
	}()
	select {
	case <-ready:
	case <-time.After(100 * time.Millisecond):
		t.Error("Transport did not timeout")
	}
}

func TestWriteTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, slowServe, stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/slow-receive", listener.Addr().String())
	request, err := http.NewRequest("POST", url, strings.NewReader(strings.Repeat("garbage890", 100000)))
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	ready := make(chan bool, 0)
	go func() {
		transport := &Transport{}
		options := &RequestOptions{
			WriteTimeout: 5 * time.Millisecond,
		}
		response, err := transport.RoundTripOptions(request, options)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				t.Log("Expected transport timeout:", neterr.Error())
			} else if err != nil {
				t.Error("Client error:", err.Error())
			}
		} else {
			response.Body.Close()
		}
		ready <- true
	}()
	select {
	case <-ready:
	case <-time.After(100 * time.Millisecond):
		t.Error("Transport did not timeout")
	}
}

func TestClose(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, delayServe, stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/delay", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	transport := &Transport{}
	conn, err := transport.GetConnRequest(request, nil)
	if err != nil {
		t.Fatal("GetConnRequest:", err.Error())
	}
	err = conn.WriteRequest(request, nil)
	if err != nil {
		t.Fatal("WriteRequest:", err.Error())
	}

	ch := make(chan *http.Response, 0)
	go func() {
		response, err := conn.ReadResponse(nil)
		if err != nil {
			t.Error("ReadResponse:", err.Error())
			ch <- nil
		} else {
			ch <- response
		}
	}()
	select {
	case <-ch:
	case <-time.After(10 * time.Millisecond):
		t.Log("Expected outer timeout, close")
		if err = conn.Close(); err != nil {
			t.Fatal("Close:", err.Error())
		}
	}

	// Check if this transport can be used to make new request to same server.
	response, err := transport.RoundTripOptions(request, nil)
	if err != nil {
		t.Fatal("Second RoundTrip:", err.Error())
	}
	if response.StatusCode != 200 {
		t.Fatal("Status is not 200")
	}
}

func TestReadLimit(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, slowServe, stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/big", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	transport := &Transport{}

	buf := new(bytes.Buffer)
	response, err := transport.RoundTripOptions(request, nil)
	if err != nil {
		t.Fatal("RoundTrip:", err.Error())
	}
	response.Header.Write(buf)
	_, err = io.Copy(buf, response.Body)
	if err != nil {
		t.Fatal("Without ReadLimit: copy Body:", err.Error())
	}
	if buf.Len() < 100 {
		t.Fatal("Without ReadLimit: expected more than 100 bytes")
	}

	buf = new(bytes.Buffer)
	response, err = transport.RoundTripOptions(request, &RequestOptions{ReadLimit: 100})
	if err != nil {
		t.Fatal("RoundTrip:", err.Error())
	}
	response.Header.Write(buf)
	_, err = io.Copy(buf, response.Body)
	if err != nil {
		t.Fatal("Without ReadLimit: copy Body:", err.Error())
	}
	if buf.Len() > 100 {
		t.Fatal("With ReadLimit: limit exceeded")
	}
}
