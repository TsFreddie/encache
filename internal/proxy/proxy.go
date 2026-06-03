package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"emby-proxy-cache/internal/interceptor"
)

const copyBufferSize = 64 * 1024

type Proxy struct {
	upstream     *url.URL
	client       *http.Client
	interceptors []interceptor.Interceptor
}

func New(upstream *url.URL, interceptors []interceptor.Interceptor) *Proxy {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}

	return &Proxy{
		upstream:     upstream,
		client:       &http.Client{Transport: transport},
		interceptors: interceptors,
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}

	upstreamURL := p.buildUpstreamURL(r.URL)
	ctx := &interceptor.Context{
		Request:     r,
		UpstreamURL: upstreamURL.String(),
	}

	response, handled, err := interceptor.RunRequest(p.interceptors, ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if !handled {
		response, err = p.forward(r, upstreamURL)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	if response == nil {
		http.Error(w, "interceptor returned nil response", http.StatusBadGateway)
		return
	}

	response, err = interceptor.RunResponse(p.interceptors, ctx, response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if response == nil {
		http.Error(w, "interceptor returned nil response", http.StatusBadGateway)
		return
	}
	if response.Body != nil {
		defer response.Body.Close()
	}

	p.writeResponse(w, r, response)
}

func (p *Proxy) buildUpstreamURL(requestURL *url.URL) *url.URL {
	upstreamURL := *p.upstream
	upstreamURL.Path = joinPath(p.upstream.Path, requestURL.Path)
	upstreamURL.RawPath = ""
	upstreamURL.RawQuery = requestURL.RawQuery
	upstreamURL.Fragment = ""
	return &upstreamURL
}

func (p *Proxy) forward(r *http.Request, upstreamURL *url.URL) (*http.Response, error) {
	request, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL.String(), r.Body)
	if err != nil {
		return nil, err
	}
	copyRequestHeaders(request.Header, r.Header)
	request.Host = upstreamURL.Host
	request.ContentLength = r.ContentLength
	return p.client.Do(request)
}

func (p *Proxy) writeResponse(w http.ResponseWriter, r *http.Request, response *http.Response) {
	copyResponseHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)

	if response.Body == nil {
		return
	}

	counter := &countingWriter{writer: w}
	buf := make([]byte, copyBufferSize)
	started := time.Now()
	lastLog := int64(0)
	nextLog := int64(16 * 1024 * 1024)
	isStream := isStreamResponse(response)

	reader := &progressReader{
		reader: response.Body,
		onProgress: func(total int64) {
			if isStream && total >= nextLog {
				interceptor.LogStreamProgress(r.URL.Path, total, counter.written, 0, started)
				lastLog = total
				nextLog += 32 * 1024 * 1024
			}
		},
	}

	_, err := io.CopyBuffer(counter, reader, buf)
	if err != nil && !isClientGone(err) && !errors.Is(r.Context().Err(), context.Canceled) {
		fmt.Printf("[HTTP] stream error %s after=%dB: %v\n", r.URL.Path, counter.written, err)
	}

	if isStream {
		fmt.Printf(
			"[HTTP] stream done %s pushed=%dB wrote=%dB in %.2fs%s\n",
			r.URL.Path,
			reader.total,
			counter.written,
			time.Since(started).Seconds(),
			finishNote(r.Context().Err(), lastLog),
		)
	}
}

func copyRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "transfer-encoding", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "upgrade":
		return true
	default:
		return false
	}
}

func joinPath(base, path string) string {
	if base == "" || base == "/" {
		return path
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
}

type countingWriter struct {
	writer  io.Writer
	written int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.written += int64(n)
	if flusher, ok := w.writer.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, err
}

type progressReader struct {
	reader     io.Reader
	total      int64
	onProgress func(int64)
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.total += int64(n)
		r.onProgress(r.total)
	}
	return n, err
}

func isStreamResponse(response *http.Response) bool {
	contentType := response.Header.Get("Content-Type")
	return response.StatusCode == http.StatusPartialContent ||
		strings.HasPrefix(contentType, "video/") ||
		contentType == "application/octet-stream"
}

func isClientGone(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "connection reset by peer") ||
		strings.Contains(message, "client disconnected")
}

func finishNote(err error, lastLog int64) string {
	if err == nil {
		return ""
	}
	return " canceled"
}
