package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"encache/internal/upstream"
)

func TestValidateContentRange(t *testing.T) {
	if err := validateContentRange("bytes 0-99/1000", 0, 99, 1000); err != nil {
		t.Fatalf("validate content range: %v", err)
	}
}

func TestValidateContentRangeRejectsMismatch(t *testing.T) {
	if err := validateContentRange("bytes 0-98/1000", 0, 99, 1000); err == nil {
		t.Fatal("expected content range mismatch")
	}
}

func TestFetchSegmentClearsFirstPendingOnEarlyFailure(t *testing.T) {
	ctx := context.Background()
	file := newTestCachedFile(t, ChunkSize)
	pending, claimed := file.AwaitOrClaim(0)
	if !claimed {
		t.Fatal("claim chunk 0 failed")
	}

	_, err := fetchSegment(ctx, file, 0, pending, FetchOptions{
		Request:     &http.Request{Method: http.MethodGet, Header: make(http.Header)},
		UpstreamURL: mustURL(t, "http://upstream/video.mkv"),
		Upstream: upstream.New(mustURL(t, "http://upstream"), nil, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("upstream failed")
		})}, 0),
	})
	if err == nil {
		t.Fatal("expected fetch error")
	}
	if file.PendingCount() != 0 {
		t.Fatalf("pending count = %d, want 0", file.PendingCount())
	}
	if file.ChunkComplete(0) {
		t.Fatal("failed chunk was marked complete")
	}
}

func TestFetchSequentialWrapsToEarlierMissingChunkAndFinalizes(t *testing.T) {
	ctx := context.Background()
	file := newTestCachedFile(t, ChunkSize*3)
	if err := file.WriteChunk(ctx, 1, chunkBytes(1, ChunkSize)); err != nil {
		t.Fatalf("write chunk 1: %v", err)
	}
	pending, claimed := file.AwaitOrClaim(2)
	if !claimed {
		t.Fatal("claim chunk 2 failed")
	}

	err := fetchSequential(ctx, file, 2, pending, FetchOptions{
		Request:     &http.Request{Method: http.MethodGet, Header: make(http.Header)},
		UpstreamURL: mustURL(t, "http://upstream/video.mkv"),
		Upstream: upstream.New(mustURL(t, "http://upstream"), nil, &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Header.Get("Range") {
			case fmt.Sprintf("bytes=%d-", ChunkSize*2):
				return partialResponse(req, ChunkSize*2, ChunkSize*3-1, ChunkSize*3, chunkBytes(2, ChunkSize)), nil
			case "bytes=0-":
				return partialResponse(req, 0, ChunkSize*3-1, ChunkSize*3, chunkBytes(0, ChunkSize)), nil
			default:
				return nil, fmt.Errorf("unexpected range %s", req.Header.Get("Range"))
			}
		})}, 0),
	})
	if err != nil {
		t.Fatalf("fetch sequential: %v", err)
	}
	if !file.Finalized() {
		t.Fatal("file was not finalized after wrap fill")
	}
	for i := 0; i < 3; i++ {
		if !file.ChunkComplete(i) {
			t.Fatalf("chunk %d incomplete", i)
		}
	}
}

func TestFetchSegmentCancelsBlockedBodyReadAndClearsPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	file := newTestCachedFile(t, ChunkSize)
	pending, claimed := file.AwaitOrClaim(0)
	if !claimed {
		t.Fatal("claim chunk 0 failed")
	}
	body := &blockingBody{readCalled: make(chan struct{}), closed: make(chan struct{})}

	done := make(chan error, 1)
	go func() {
		_, err := fetchSegment(ctx, file, 0, pending, FetchOptions{
			Request:     &http.Request{Method: http.MethodGet, Header: make(http.Header)},
			UpstreamURL: mustURL(t, "http://upstream/video.mkv"),
			Upstream: upstream.New(mustURL(t, "http://upstream"), nil, &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return partialResponseWithBody(req, 0, ChunkSize-1, ChunkSize, body), nil
			})}, 0),
		})
		done <- err
	}()

	body.waitForRead(t)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected canceled fetch error")
		}
	case <-time.After(time.Second):
		t.Fatal("fetch did not return after context cancellation")
	}
	if file.PendingCount() != 0 {
		t.Fatalf("pending count = %d, want 0", file.PendingCount())
	}
	if file.ChunkComplete(0) {
		t.Fatal("canceled chunk was marked complete")
	}
}

func TestActiveReadCanFetchCurrentChunkDuringReadahead(t *testing.T) {
	ctx := context.Background()
	file := newTestCachedFile(t, ChunkSize*3)
	if err := file.WriteChunk(ctx, 0, chunkBytes(0, ChunkSize)); err != nil {
		t.Fatalf("write chunk 0: %v", err)
	}
	pendingChunk1, claimed := file.AwaitOrClaim(1)
	if !claimed {
		t.Fatal("claim chunk 1 failed")
	}

	chunk2ReadStarted := make(chan struct{})
	chunk2Closed := make(chan struct{})
	chunk1Requested := make(chan struct{})
	chunk1StartedBeforeChunk2Closed := false
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.Header.Get("Range") {
		case fmt.Sprintf("bytes=%d-", ChunkSize):
			select {
			case <-chunk2Closed:
			default:
				chunk1StartedBeforeChunk2Closed = true
			}
			close(chunk1Requested)
			return partialResponse(req, ChunkSize, ChunkSize*3-1, ChunkSize*3, append(chunkBytes(1, ChunkSize), chunkBytes(2, ChunkSize)...)), nil
		case fmt.Sprintf("bytes=%d-", ChunkSize*2):
			return partialResponseWithBody(req, ChunkSize*2, ChunkSize*3-1, ChunkSize*3, &blockingBody{readCalled: chunk2ReadStarted, closed: chunk2Closed}), nil
		default:
			return nil, fmt.Errorf("unexpected range %s", req.Header.Get("Range"))
		}
	})}

	reader, err := file.ReadRange(ctx, 0, ChunkSize*3-1, FetchOptions{
		Class:       SessionActive,
		Request:     &http.Request{Method: http.MethodGet, Header: make(http.Header)},
		UpstreamURL: mustURL(t, "http://upstream/video.mkv"),
		Upstream:    upstream.New(mustURL(t, "http://upstream"), nil, client, 0),
	})
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	defer reader.Close()

	buf := make([]byte, 1)
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}

	select {
	case <-chunk2ReadStarted:
	case <-time.After(time.Second):
		t.Fatal("readahead fetch did not start")
	}
	file.CompleteFetch(1, pendingChunk1, context.Canceled)

	if _, err := reader.Read(make([]byte, ChunkSize-1)); err != nil {
		t.Fatalf("read rest of first chunk: %v", err)
	}
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("read current chunk: %v", err)
	}
	select {
	case <-chunk1Requested:
	case <-time.After(time.Second):
		t.Fatal("current chunk did not fetch while readahead was blocked")
	}
	if chunk1StartedBeforeChunk2Closed {
		t.Fatal("current chunk fetch started before readahead upstream closed")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTestCachedFile(t *testing.T, size int64) *CachedFile {
	t.Helper()
	file, err := OpenCachedFile(context.Background(), t.TempDir(), testMediaSource(size), &fakeChunkStore{})
	if err != nil {
		t.Fatalf("open cached file: %v", err)
	}
	t.Cleanup(func() { _ = file.Close(context.Background()) })
	return file
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return u
}

func partialResponse(req *http.Request, start, end, total int64, body []byte) *http.Response {
	return partialResponseWithBody(req, start, end, total, io.NopCloser(bytes.NewReader(body)))
}

func partialResponseWithBody(req *http.Request, start, end, total int64, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Header: http.Header{
			"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", start, end, total)},
		},
		Body:    body,
		Request: req,
	}
}

type blockingBody struct {
	once       sync.Once
	readCalled chan struct{}
	closed     chan struct{}
}

func (b *blockingBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.readCalled) })
	<-b.closed
	return 0, context.Canceled
}

func (b *blockingBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func (b *blockingBody) waitForRead(t *testing.T) {
	t.Helper()
	select {
	case <-b.readCalled:
	case <-time.After(time.Second):
		t.Fatal("body read did not start")
	}
}

func chunkBytes(chunk int, size int64) []byte {
	data := bytes.Repeat([]byte{byte(chunk + 1)}, int(size))
	return data
}
