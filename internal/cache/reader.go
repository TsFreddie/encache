package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type SessionClass string

const SessionActive SessionClass = "active"

type FetchOptions struct {
	Class       SessionClass
	Request     *http.Request
	UpstreamURL *url.URL
	Client      *http.Client
}

type rangeReader struct {
	ctx     context.Context
	file    *CachedFile
	fetch   FetchOptions
	start   int64
	end     int64
	offset  int64
	current *bytes.Reader
	closed  bool
}

func (f *CachedFile) ReadRange(ctx context.Context, start, end int64, fetch FetchOptions) (io.ReadCloser, error) {
	if start < 0 || end < start || end >= f.Size() {
		return nil, fmt.Errorf("invalid range %d-%d/%d", start, end, f.Size())
	}
	return &rangeReader{
		ctx:    ctx,
		file:   f,
		fetch:  fetch,
		start:  start,
		end:    end,
		offset: start,
	}, nil
}

func (r *rangeReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.offset > r.end {
		return 0, io.EOF
	}

	for r.current == nil || r.current.Len() == 0 {
		if err := r.loadCurrentChunk(); err != nil {
			return 0, err
		}
	}

	n, err := r.current.Read(p)
	r.offset += int64(n)
	if err == io.EOF && r.offset <= r.end {
		err = nil
	}
	return n, err
}

func (r *rangeReader) Close() error {
	r.closed = true
	return nil
}

func (r *rangeReader) loadCurrentChunk() error {
	chunkIndex := int(r.offset / ChunkSize)
	if err := r.ensureChunk(chunkIndex); err != nil {
		return err
	}

	chunk, err := r.file.ReadChunk(chunkIndex)
	if err != nil {
		return err
	}
	chunkStart := int64(chunkIndex) * ChunkSize
	from := r.offset - chunkStart
	to := int64(len(chunk))
	if chunkStart+to-1 > r.end {
		to = r.end - chunkStart + 1
	}
	r.current = bytes.NewReader(chunk[from:to])
	return nil
}

func (r *rangeReader) ensureChunk(index int) error {
	if r.file.ChunkComplete(index) {
		return nil
	}
	pending, claimed := r.file.AwaitOrClaim(index)
	if !claimed {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-pending.done:
			return pending.err
		}
	}

	err := fetchChunk(r.ctx, r.file, index, r.fetch)
	r.file.CompleteFetch(index, pending, err)
	return err
}

func fetchChunk(ctx context.Context, file *CachedFile, index int, options FetchOptions) error {
	if options.Request == nil || options.UpstreamURL == nil {
		return fmt.Errorf("missing upstream request details")
	}
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}

	start, end := file.ChunkBounds(index)
	request, err := http.NewRequestWithContext(ctx, options.Request.Method, options.UpstreamURL.String(), nil)
	if err != nil {
		return err
	}
	copyFetchHeaders(request.Header, options.Request.Header)
	request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	request.Host = options.UpstreamURL.Host

	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("upstream status %d, want 206", response.StatusCode)
	}
	if err := validateContentRange(response.Header.Get("Content-Range"), start, end, file.Size()); err != nil {
		return err
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, end-start+2))
	if err != nil {
		return err
	}
	if int64(len(data)) != end-start+1 {
		return fmt.Errorf("upstream chunk length %d, want %d", len(data), end-start+1)
	}
	return file.WriteChunk(ctx, index, data)
}

func validateContentRange(header string, start, end, total int64) error {
	expected := fmt.Sprintf("bytes %d-%d/%d", start, end, total)
	if strings.TrimSpace(header) != expected {
		return fmt.Errorf("content-range %q, want %q", header, expected)
	}
	return nil
}

func copyFetchHeaders(dst, src http.Header) {
	for key, values := range src {
		if isFetchHopHeader(key) || strings.EqualFold(key, "Range") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isFetchHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "transfer-encoding", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "upgrade":
		return true
	default:
		return false
	}
}
