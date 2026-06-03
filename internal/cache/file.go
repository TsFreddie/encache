package cache

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"emby-proxy-cache/internal/store"
)

const (
	ChunkSize              int64 = 4 * 1024 * 1024
	chunkStateSaveInterval       = 5 * time.Second
)

type chunkFetch struct {
	done chan struct{}
	err  error
}

type CachedFile struct {
	mu                 sync.Mutex
	path               string
	progressPath       string
	file               *os.File
	source             store.MediaSource
	chunks             *Bitset
	store              chunkStore
	lastChunkStateSave time.Time
	finalized          bool
	pending            map[int]*chunkFetch
}

type chunkStore interface {
	UpdateChunks(ctx context.Context, mediaSourceID string, chunks []byte) error
}

func OpenCachedFile(ctx context.Context, storagePath string, source store.MediaSource, store chunkStore) (*CachedFile, error) {
	dir := filepath.Join(storagePath, source.ItemName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, source.SourceName+"."+source.Container)
	progressPath := path + ".progress"
	chunkCount := chunkCount(source.Size)

	if info, err := os.Stat(path); err == nil && info.Size() == source.Size {
		file, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		chunks := NewBitset(chunkCount)
		for i := 0; i < chunkCount; i++ {
			chunks.Set(i, true)
		}
		return &CachedFile{
			path:         path,
			progressPath: progressPath,
			file:         file,
			source:       source,
			chunks:       chunks,
			store:        store,
			finalized:    true,
		}, nil
	}
	if info, err := os.Stat(path); err == nil && info.Size() != source.Size {
		_ = os.Remove(path)
	}

	chunks := NewBitset(chunkCount)
	progressInfo, progressErr := os.Stat(progressPath)
	progressExists := progressErr == nil && progressInfo.Size() == source.Size
	if progressErr == nil && progressInfo.Size() != source.Size {
		_ = os.Remove(progressPath)
		progressExists = false
	}

	if progressExists && len(source.Chunks) > 0 {
		decoded, err := BitsetFromBytes(source.Chunks)
		if err == nil && decoded.Size() == chunkCount {
			chunks = decoded
			if chunks.Complete() {
				file, err := os.OpenFile(progressPath, os.O_RDWR, 0)
				if err != nil {
					return nil, err
				}
				cached := &CachedFile{
					path:         path,
					progressPath: progressPath,
					file:         file,
					source:       source,
					chunks:       chunks,
					store:        store,
					pending:      make(map[int]*chunkFetch),
				}
				if err := cached.finalizeLocked(ctx); err != nil {
					_ = file.Close()
					return nil, err
				}
				return cached, nil
			}
		} else {
			_ = os.Remove(progressPath)
			progressExists = false
		}
	}
	if !progressExists {
		if store != nil {
			if err := store.UpdateChunks(ctx, source.MediaSourceID, nil); err != nil {
				return nil, err
			}
		}
		chunks = NewBitset(chunkCount)
	}

	file, err := os.OpenFile(progressPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(source.Size); err != nil {
		_ = file.Close()
		return nil, err
	}

	return &CachedFile{
		path:         path,
		progressPath: progressPath,
		file:         file,
		source:       source,
		chunks:       chunks,
		store:        store,
		pending:      make(map[int]*chunkFetch),
	}, nil
}

func (f *CachedFile) Close(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.finalized {
		if err := f.saveChunksLocked(ctx); err != nil {
			_ = f.file.Close()
			return err
		}
	}
	return f.file.Close()
}

func (f *CachedFile) Size() int64 {
	return f.source.Size
}

func (f *CachedFile) Source() store.MediaSource {
	return f.source
}

func (f *CachedFile) ChunkCount() int {
	return f.chunks.Size()
}

func (f *CachedFile) ChunkBounds(index int) (int64, int64) {
	start := int64(index) * ChunkSize
	end := min(start+ChunkSize, f.source.Size) - 1
	return start, end
}

func (f *CachedFile) ChunkComplete(index int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.chunks.Get(index)
}

func (f *CachedFile) Finalized() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.finalized
}

func (f *CachedFile) ReadAt(p []byte, offset int64) (int, error) {
	return f.file.ReadAt(p, offset)
}

func (f *CachedFile) ReadChunk(index int) ([]byte, error) {
	start, end := f.ChunkBounds(index)
	buf := make([]byte, end-start+1)
	n, err := f.file.ReadAt(buf, start)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("read chunk %d: got %d bytes, want %d", index, n, len(buf))
	}
	return buf, nil
}

func (f *CachedFile) WriteChunk(ctx context.Context, index int, data []byte) error {
	start, end := f.ChunkBounds(index)
	expected := int(end - start + 1)
	if len(data) != expected {
		return fmt.Errorf("write chunk %d: got %d bytes, want %d", index, len(data), expected)
	}

	n, err := f.file.WriteAt(data, start)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("write chunk %d: wrote %d bytes, want %d", index, n, len(data))
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.chunks.Get(index) {
		return nil
	}
	f.chunks.Set(index, true)
	if f.chunks.Complete() {
		fmt.Printf("[StreamCache] cache complete mediaSource=%s chunks=%d\n", f.source.MediaSourceID, f.chunks.Size())
		return f.finalizeLocked(ctx)
	}
	if time.Since(f.lastChunkStateSave) >= chunkStateSaveInterval {
		return f.saveChunksLocked(ctx)
	}
	return nil
}

func (f *CachedFile) AwaitOrClaim(index int) (*chunkFetch, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.chunks.Get(index) {
		return nil, false
	}
	if pending, ok := f.pending[index]; ok {
		return pending, false
	}
	pending := &chunkFetch{done: make(chan struct{})}
	f.pending[index] = pending
	return pending, true
}

func (f *CachedFile) AwaitChunk(index int) *chunkFetch {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.chunks.Get(index) {
		return nil
	}
	return f.pending[index]
}

func (f *CachedFile) ClaimNextMissingFrom(index int) (int, *chunkFetch, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := index; i < f.chunks.Size(); i++ {
		if f.chunks.Get(i) {
			continue
		}
		if _, ok := f.pending[i]; ok {
			continue
		}
		pending := &chunkFetch{done: make(chan struct{})}
		f.pending[i] = pending
		return i, pending, true
	}
	return 0, nil, false
}

func (f *CachedFile) PendingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending)
}

func (f *CachedFile) CompleteFetch(index int, pending *chunkFetch, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if current, ok := f.pending[index]; ok && current == pending {
		pending.err = err
		delete(f.pending, index)
		close(pending.done)
	}
}

func (f *CachedFile) saveChunksLocked(ctx context.Context) error {
	if f.store == nil {
		return nil
	}
	if err := f.store.UpdateChunks(ctx, f.source.MediaSourceID, f.chunks.Bytes()); err != nil {
		return err
	}
	f.lastChunkStateSave = time.Now()
	return nil
}

func (f *CachedFile) finalizeLocked(ctx context.Context) error {
	if f.finalized {
		return nil
	}
	if err := f.saveChunksLocked(ctx); err != nil {
		return err
	}
	if err := f.file.Sync(); err != nil {
		return err
	}
	if err := os.Rename(f.progressPath, f.path); err != nil {
		return err
	}
	finalFile, err := os.OpenFile(f.path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := f.file.Close(); err != nil {
		_ = finalFile.Close()
		return err
	}
	f.file = finalFile
	f.finalized = true
	fmt.Printf("[StreamCache] cache finalized mediaSource=%s path=%q\n", f.source.MediaSourceID, f.path)
	return nil
}

func chunkCount(size int64) int {
	return int((size + ChunkSize - 1) / ChunkSize)
}
