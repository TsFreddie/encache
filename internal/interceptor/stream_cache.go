package interceptor

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"emby-proxy-cache/internal/cache"
)

var videoStreamPath = regexp.MustCompile(`^/emby/videos/[0-9]+/(stream|original)\.([a-zA-Z0-9]+)$`)

type StreamCache struct {
	Base
	Cache *cache.Manager
}

func (s StreamCache) OnRequest(ctx *Context) (*http.Response, bool, error) {
	if s.Cache == nil {
		return nil, false, nil
	}
	req := ctx.Request
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return nil, false, nil
	}
	matches := videoStreamPath.FindStringSubmatch(req.URL.Path)
	if matches == nil {
		return nil, false, nil
	}

	mediaSourceID := firstQuery(req, "MediaSourceId", "mediasourceid")
	if mediaSourceID == "" {
		return nil, false, nil
	}
	rangeHeader := req.Header.Get("Range")
	if rangeHeader == "" {
		return nil, false, nil
	}

	handle, err := s.Cache.Open(req.Context(), mediaSourceID)
	if err != nil {
		if err == cache.ErrMediaSourceNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}

	start, end, err := parseByteRange(rangeHeader, handle.Source.Size)
	if err != nil {
		_ = handle.Close()
		return rangeNotSatisfiable(req, handle.Source.Size), true, nil
	}
	upstreamURL, err := parseUpstreamURL(ctx.UpstreamURL)
	if err != nil {
		_ = handle.Close()
		return nil, false, err
	}

	reader, err := handle.File.ReadRange(req.Context(), start, end, cache.FetchOptions{
		Class:       cache.SessionActive,
		Request:     req,
		UpstreamURL: upstreamURL,
		Client:      s.Cache.Client,
	})
	if err != nil {
		_ = handle.Close()
		return nil, false, err
	}

	body := closeBoth{Reader: reader, CloseFunc: handle.Close}
	if req.Method == http.MethodHead {
		_ = body.Close()
		body = closeBoth{Reader: http.NoBody, CloseFunc: func() error { return nil }}
	}

	response := &http.Response{
		StatusCode:    http.StatusPartialContent,
		Status:        "206 Partial Content",
		Header:        make(http.Header),
		Body:          body,
		ContentLength: end - start + 1,
		Request:       req,
	}
	response.Header.Set("Accept-Ranges", "bytes")
	response.Header.Set("Cache-Control", "private, no-transform")
	response.Header.Set("Content-Length", strconv.FormatInt(response.ContentLength, 10))
	response.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, handle.Source.Size))
	response.Header.Set("Content-Type", contentTypeForContainer(matches[2]))

	fmt.Printf("[StreamCache] active mediaSource=%s range=%d-%d/%d\n", mediaSourceID, start, end, handle.Source.Size)
	return response, true, nil
}

func parseUpstreamURL(raw string) (*url.URL, error) {
	upstreamURL, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	return upstreamURL, nil
}

type closeBoth struct {
	Reader interface {
		Read([]byte) (int, error)
		Close() error
	}
	CloseFunc func() error
}

func (c closeBoth) Read(p []byte) (int, error) {
	return c.Reader.Read(p)
}

func (c closeBoth) Close() error {
	err := c.Reader.Close()
	if closeErr := c.CloseFunc(); err == nil {
		err = closeErr
	}
	return err
}

func firstQuery(req *http.Request, names ...string) string {
	query := req.URL.Query()
	for _, name := range names {
		if value := query.Get(name); value != "" {
			return value
		}
	}
	for key, values := range query {
		for _, name := range names {
			if strings.EqualFold(key, name) && len(values) > 0 {
				return values[0]
			}
		}
	}
	return ""
}

func parseByteRange(header string, size int64) (int64, int64, error) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("unsupported range %q", header)
	}
	spec := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, fmt.Errorf("multiple ranges unsupported")
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range %q", header)
	}

	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, fmt.Errorf("invalid suffix range %q", header)
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, nil
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, fmt.Errorf("invalid range start %q", header)
	}
	end := size - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return 0, 0, fmt.Errorf("invalid range end %q", header)
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end, nil
}

func rangeNotSatisfiable(req *http.Request, size int64) *http.Response {
	response := &http.Response{
		StatusCode: http.StatusRequestedRangeNotSatisfiable,
		Status:     "416 Range Not Satisfiable",
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    req,
	}
	response.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
	return response
}

func contentTypeForContainer(container string) string {
	switch strings.ToLower(container) {
	case "mp4", "m4v":
		return "video/mp4"
	case "webm":
		return "video/webm"
	case "mkv":
		return "video/x-matroska"
	default:
		return "application/octet-stream"
	}
}
