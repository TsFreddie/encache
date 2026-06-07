package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const fillProgressLogInterval = 5 * time.Second

type SessionClass string

const (
	SessionActive  SessionClass = "active"
	SessionPassive SessionClass = "passive"
)

type FetchOptions struct {
	Class       SessionClass
	Request     *http.Request
	UpstreamURL *url.URL
	Client      *http.Client
	Gate        *DownloadGate
}

type rangeReader struct {
	ctx     context.Context
	file    *CachedFile
	fetch   FetchOptions
	cancel  context.CancelFunc
	once    sync.Once
	fillMu  sync.Mutex
	filling bool
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
	firstChunk := int(start / ChunkSize)
	lastChunk := int(end / ChunkSize)
	fmt.Printf(
		"[StreamCache] client range mediaSource=%s start=%d end=%d firstChunk=%d lastChunk=%d totalChunks=%d\n",
		f.source.MediaSourceID,
		start,
		end,
		firstChunk,
		lastChunk,
		f.ChunkCount(),
	)
	readCtx, cancel := context.WithCancel(ctx)
	if fetch.Class == SessionActive && fetch.Gate != nil {
		release := fetch.Gate.ActiveStarted()
		baseCancel := cancel
		cancel = func() {
			release()
			baseCancel()
		}
	}
	return &rangeReader{
		ctx:    readCtx,
		file:   f,
		fetch:  fetch,
		cancel: cancel,
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
	r.once.Do(r.cancel)
	r.fillMu.Lock()
	r.closed = true
	r.fillMu.Unlock()
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
	for {
		if r.file.ChunkComplete(index) {
			r.startFillFromNextMissing(index + 1)
			return nil
		}
		pending, claimed := r.file.AwaitOrClaim(index)
		if claimed {
			fmt.Printf("[StreamCache] fill current mediaSource=%s chunk=%d\n", r.file.Source().MediaSourceID, index)
			return r.fetchCurrentChunk(index, pending)
		}
		if pending == nil {
			continue
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-pending.done:
			if pending.err == nil {
				return nil
			}
			if r.ctx.Err() == nil && (errors.Is(pending.err, context.Canceled) || errors.Is(pending.err, context.DeadlineExceeded)) {
				fmt.Printf("[StreamCache] retry canceled pending mediaSource=%s chunk=%d\n", r.file.Source().MediaSourceID, index)
				continue
			}
			return pending.err
		}
	}
}

func (r *rangeReader) fetchCurrentChunk(index int, pending *chunkFetch) error {
	r.fillMu.Lock()
	if r.filling {
		r.fillMu.Unlock()
		filePending := r.file.AwaitChunk(index)
		if filePending == pending {
			select {
			case <-r.ctx.Done():
				return r.ctx.Err()
			case <-pending.done:
				return pending.err
			}
		}
		return nil
	}
	r.filling = true
	r.fillMu.Unlock()

	go func() {
		fetchFromChunk(r.ctx, r.file, index, pending, r.fetch)
		r.fillMu.Lock()
		r.filling = false
		r.fillMu.Unlock()
	}()
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-pending.done:
		return pending.err
	}
}

func (r *rangeReader) startFillFromNextMissing(index int) {
	r.fillMu.Lock()
	if r.filling {
		r.fillMu.Unlock()
		return
	}
	missingIndex, pending, ok := r.file.ClaimNextMissingFrom(index)
	if !ok {
		r.fillMu.Unlock()
		return
	}
	r.filling = true
	r.fillMu.Unlock()

	fmt.Printf("[StreamCache] fill readahead mediaSource=%s fromChunk=%d requestedChunk=%d\n", r.file.Source().MediaSourceID, missingIndex, index)
	go func() {
		fetchFromChunk(r.ctx, r.file, missingIndex, pending, r.fetch)
		r.fillMu.Lock()
		r.filling = false
		r.fillMu.Unlock()
	}()
}

func fetchFromChunk(ctx context.Context, file *CachedFile, startIndex int, firstPending *chunkFetch, options FetchOptions) {
	err := fetchSequential(ctx, file, startIndex, firstPending, options)
	if err != nil {
		fmt.Printf("[StreamCache] fill failed mediaSource=%s chunk=%d: %v\n", file.Source().MediaSourceID, startIndex, err)
	}
}

func fetchSequential(ctx context.Context, file *CachedFile, startIndex int, firstPending *chunkFetch, options FetchOptions) error {
	if options.Class == SessionPassive && options.Gate != nil {
		release, err := options.Gate.WaitDownloadTurn(ctx)
		if err != nil {
			return err
		}
		defer release()
	}

	nextIndex := startIndex
	nextPending := firstPending
	for nextPending != nil {
		resumeIndex, err := fetchSegment(ctx, file, nextIndex, nextPending, options)
		if err != nil {
			return err
		}
		if resumeIndex >= file.ChunkCount() {
			if file.Finalized() {
				return nil
			}
			var ok bool
			nextIndex, nextPending, ok = file.ClaimNextMissingFrom(0)
			if !ok {
				return nil
			}
			fmt.Printf("[StreamCache] fill wrap mediaSource=%s fromChunk=%d\n", file.Source().MediaSourceID, nextIndex)
			continue
		}
		var ok bool
		nextIndex, nextPending, ok = file.ClaimNextMissingFrom(resumeIndex)
		if !ok {
			if file.Finalized() {
				return nil
			}
			nextIndex, nextPending, ok = file.ClaimNextMissingFrom(0)
			if !ok {
				return nil
			}
			fmt.Printf("[StreamCache] fill wrap mediaSource=%s fromChunk=%d\n", file.Source().MediaSourceID, nextIndex)
			continue
		}
		fmt.Printf("[StreamCache] fill gap mediaSource=%s fromChunk=%d afterChunk=%d\n", file.Source().MediaSourceID, nextIndex, resumeIndex-1)
	}
	return nil
}

func fetchSegment(ctx context.Context, file *CachedFile, startIndex int, firstPending *chunkFetch, options FetchOptions) (int, error) {
	fail := func(err error) (int, error) {
		file.CompleteFetch(startIndex, firstPending, err)
		return startIndex, err
	}

	if options.Request == nil || options.UpstreamURL == nil {
		return fail(fmt.Errorf("missing upstream request details"))
	}
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}

	start, _ := file.ChunkBounds(startIndex)
	request, err := http.NewRequestWithContext(ctx, options.Request.Method, options.UpstreamURL.String(), nil)
	if err != nil {
		return fail(err)
	}
	copyFetchHeaders(request.Header, options.Request.Header)
	request.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	request.Host = options.UpstreamURL.Host

	response, err := client.Do(request)
	if err != nil {
		return fail(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		return fail(fmt.Errorf("upstream status %d, want 206", response.StatusCode))
	}
	if err := validateContentRangeStart(response.Header.Get("Content-Range"), start, file.Size()); err != nil {
		return fail(err)
	}
	progress := newFillProgress(file.Source().MediaSourceID, startIndex, start, file.Size())
	progress.Start(response.Header.Get("Content-Length"), response.Header.Get("Content-Range"))

	for index := startIndex; index < file.ChunkCount(); index++ {
		_, end := file.ChunkBounds(index)
		expected := end - int64(index)*ChunkSize + 1
		pending := firstPending
		claimed := true
		if index != startIndex {
			pending, claimed = file.AwaitOrClaim(index)
			if !claimed {
				progress.HitCachedBlock(index)
				progress.Done()
				return index, nil
			}
		}
		if options.Class == SessionPassive && options.Gate != nil {
			if err := options.Gate.WaitDownloadResumed(ctx); err != nil {
				file.CompleteFetch(index, pending, err)
				return index, err
			}
		}

		data, err := readExactly(ctx, response.Body, expected, progress)
		if err != nil {
			file.CompleteFetch(index, pending, err)
			return index, err
		}
		err = file.WriteChunk(ctx, index, data)
		file.CompleteFetch(index, pending, err)
		if err != nil {
			return index, err
		}
		progress.CompleteChunk(expected)
	}
	progress.Done()
	return file.ChunkCount(), nil
}

func validateContentRange(header string, start, end, total int64) error {
	expected := fmt.Sprintf("bytes %d-%d/%d", start, end, total)
	if strings.TrimSpace(header) != expected {
		return fmt.Errorf("content-range %q, want %q", header, expected)
	}
	return nil
}

func validateContentRangeStart(header string, start, total int64) error {
	prefix := fmt.Sprintf("bytes %d-", start)
	suffix := fmt.Sprintf("/%d", total)
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, prefix) || !strings.HasSuffix(header, suffix) {
		return fmt.Errorf("content-range %q, want %q...%q", header, prefix, suffix)
	}
	return nil
}

func readExactly(ctx context.Context, reader io.Reader, size int64, progress *fillProgress) ([]byte, error) {
	if size > int64(int(size)) {
		return nil, fmt.Errorf("read size %d overflows int", size)
	}
	data := make([]byte, int(size))
	read := 0
	for read < len(data) {
		n, err := reader.Read(data[read:])
		if n > 0 {
			read += n
			if progress != nil {
				progress.Download(int64(n))
			}
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if err == io.EOF && read == len(data) {
				break
			}
			return nil, err
		}
	}
	return data, nil
}

type fillProgress struct {
	mediaSourceID string
	startChunk    int
	startByte     int64
	totalSize     int64
	started       time.Time
	lastLog       time.Time
	downloaded    int64
	cached        int64
}

func newFillProgress(mediaSourceID string, startChunk int, startByte, totalSize int64) *fillProgress {
	now := time.Now()
	return &fillProgress{
		mediaSourceID: mediaSourceID,
		startChunk:    startChunk,
		startByte:     startByte,
		totalSize:     totalSize,
		started:       now,
		lastLog:       now,
	}
}

func (p *fillProgress) Start(contentLength, contentRange string) {
	fmt.Printf(
		"[StreamCache] fill start mediaSource=%s chunk=%d offset=%d remaining=%d contentLength=%s contentRange=%q\n",
		p.mediaSourceID,
		p.startChunk,
		p.startByte,
		p.totalSize-p.startByte,
		contentLength,
		contentRange,
	)
}

func (p *fillProgress) Download(bytes int64) {
	p.downloaded += bytes
	if time.Since(p.lastLog) < fillProgressLogInterval {
		return
	}
	p.lastLog = time.Now()
	fmt.Printf(
		"[StreamCache] fill progress mediaSource=%s downloaded=%.1fMB cached=%.1fMB speed=%.2fMB/s remaining=%.1fMB\n",
		p.mediaSourceID,
		mb(p.downloaded),
		mb(p.cached),
		mbps(p.downloaded, p.started),
		mb(max(p.totalSize-p.startByte-p.downloaded, 0)),
	)
}

func (p *fillProgress) CompleteChunk(bytes int64) {
	p.cached += bytes
}

func (p *fillProgress) HitCachedBlock(index int) {
	fmt.Printf(
		"[StreamCache] fill stop mediaSource=%s cachedBlockChunk=%d downloaded=%.1fMB cached=%.1fMB speed=%.2fMB/s\n",
		p.mediaSourceID,
		index,
		mb(p.downloaded),
		mb(p.cached),
		mbps(p.downloaded, p.started),
	)
}

func (p *fillProgress) Done() {
	fmt.Printf(
		"[StreamCache] fill done mediaSource=%s downloaded=%.1fMB cached=%.1fMB speed=%.2fMB/s elapsed=%.2fs\n",
		p.mediaSourceID,
		mb(p.downloaded),
		mb(p.cached),
		mbps(p.downloaded, p.started),
		time.Since(p.started).Seconds(),
	)
}

func mb(bytes int64) float64 {
	return float64(bytes) / 1024 / 1024
}

func mbps(bytes int64, started time.Time) float64 {
	seconds := time.Since(started).Seconds()
	if seconds <= 0 {
		seconds = 0.001
	}
	return mb(bytes) / seconds
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
