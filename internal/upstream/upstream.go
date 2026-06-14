package upstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"encache/internal/logging"
)

type Upstream struct {
	Primary          *url.URL
	Fallback         *url.URL
	Client           *http.Client
	stickyUntil      time.Time
	stickyMu         sync.Mutex
	fallbackDuration time.Duration
}

type Request struct {
	Method        string
	URL           *url.URL
	Body          io.ReadCloser
	GetBody       func() (io.ReadCloser, error)
	Header        http.Header
	ContentLength int64
	NoFallback    bool
	cachedBody    []byte // buffered body for retry replay
}

func New(primary, fallback *url.URL, client *http.Client, fallbackDuration time.Duration) *Upstream {
	if client == nil {
		client = NewClient()
	}
	return &Upstream{
		Primary:          primary,
		Fallback:         fallback,
		Client:           client,
		fallbackDuration: fallbackDuration,
	}
}

func (u *Upstream) Do(ctx context.Context, req *Request) (*http.Response, error) {
	// Check sticky fallback before trying primary
	if u.isFallbackSticky() {
		logging.Verbosef("[Upstream] sticky fallback active, skipping primary\n")
		return u.doWithBase(ctx, req, true)
	}
	return u.doWithBase(ctx, req, false)
}

func (u *Upstream) DoFallback(ctx context.Context, req *Request) (*http.Response, error) {
	return u.doWithBase(ctx, req, true)
}

func IsNetworkError(err error) bool {
	return isNetworkError(err)
}

// MarkFallback records that fallback succeeded; primary won't be used for
// fallbackDuration from now.
func (u *Upstream) MarkFallback() {
	u.stickyMu.Lock()
	defer u.stickyMu.Unlock()
	u.stickyUntil = time.Now().Add(u.fallbackDuration)
	logging.Verbosef("[Upstream] marked sticky fallback until %s (duration=%s)\n",
		u.stickyUntil.Format(time.RFC3339Nano), u.fallbackDuration)
}

// isFallbackSticky returns true if primary should be skipped in favor of fallback.
func (u *Upstream) isFallbackSticky() bool {
	if u.Fallback == nil || u.fallbackDuration <= 0 {
		return false
	}
	u.stickyMu.Lock()
	defer u.stickyMu.Unlock()
	return time.Now().Before(u.stickyUntil)
}

func (u *Upstream) doWithBase(ctx context.Context, req *Request, isFallback bool) (*http.Response, error) {
	base := u.Primary
	if isFallback {
		base = u.Fallback
	}

	var requestURL *url.URL
	if isFallback {
		requestURL = u.buildURL(base, req.URL)
	} else if req.URL.IsAbs() {
		requestURL = req.URL
	} else {
		requestURL = u.buildURL(base, req.URL)
	}

	var body io.Reader
	if isFallback {
		if req.GetBody != nil {
			rc, _ := req.GetBody()
			body = rc
		} else if req.cachedBody != nil {
			body = bytes.NewReader(req.cachedBody)
		} else {
			body = req.Body
		}
	} else {
		body = req.Body
		if req.GetBody != nil {
			if freshBody, bodyErr := req.GetBody(); bodyErr == nil {
				body = freshBody
			}
		}
		// Buffer body before first send so fallback retry can replay it.
		// Only buffer if fallback is available and not disabled.
		if body != nil && u.Fallback != nil && !req.NoFallback && req.cachedBody == nil {
			var buf bytes.Buffer
			_, readErr := io.Copy(&buf, body.(io.Reader))
			if closer, ok := body.(io.Closer); ok {
				closer.Close()
			}
			if readErr != nil {
				return nil, readErr
			}
			req.cachedBody = buf.Bytes()
			body = bytes.NewReader(req.cachedBody)
		}
	}
	request, err := http.NewRequestWithContext(ctx, req.Method, requestURL.String(), body)
	if err != nil {
		return nil, err
	}
	copyHeaders(request.Header, req.Header)
	request.Host = requestURL.Host
	if req.ContentLength > 0 {
		request.ContentLength = req.ContentLength
	}
	response, err := u.Client.Do(request)
	if err != nil {
		if u.Fallback != nil && !isFallback && !req.NoFallback && isNetworkError(err) {
			logging.Verbosef("[Upstream] failed %s: %v — retrying via fallback %s\n", request.URL.String(), err, u.Fallback.String())
			resp, fbErr := u.doWithBase(ctx, req, true)
			if fbErr != nil {
				logging.Verbosef("[Upstream] fallback also failed %s: %v\n", request.URL.String(), fbErr)
				return nil, fbErr
			}
			logging.Verbosef("[Upstream] fallback succeeded %s\n", request.URL.String())
			u.MarkFallback()
			return resp, nil
		}
		return nil, err
	}
	return response, nil
}

func (u *Upstream) BuildURL(requestURL *url.URL, isFallback bool) *url.URL {
	base := u.Primary
	if isFallback {
		base = u.Fallback
	}
	return u.buildURL(base, requestURL)
}

func (u *Upstream) buildURL(base *url.URL, requestURL *url.URL) *url.URL {
	upstreamURL := *base
	upstreamURL.Path = joinPath(base.Path, requestURL.Path)
	upstreamURL.RawQuery = requestURL.RawQuery
	upstreamURL.Fragment = ""
	return &upstreamURL
}

func copyHeaders(dst, src http.Header) {
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
	switch key {
	case "Connection", "Transfer-Encoding", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Upgrade":
		return true
	}
	return false
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	return false
}

func joinPath(base, path string) string {
	if base == "" || base == "/" {
		return path
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
}
