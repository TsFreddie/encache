package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"emcache/internal/store"
)

type fakeChunkStore struct {
	chunks  []byte
	updates [][]byte
}

func (s *fakeChunkStore) UpdateChunks(_ context.Context, _ string, chunks []byte) error {
	s.chunks = chunks
	s.updates = append(s.updates, chunks)
	return nil
}

func TestOpenCachedFileResetsChunksWhenFilesMissing(t *testing.T) {
	ctx := context.Background()
	source := testMediaSource(ChunkSize * 2)
	store := &fakeChunkStore{chunks: completeChunks(source.Size)}

	file, err := OpenCachedFile(ctx, t.TempDir(), source, store)
	if err != nil {
		t.Fatalf("open cached file: %v", err)
	}
	if err := file.Close(ctx); err != nil {
		t.Fatalf("close cached file: %v", err)
	}
	if len(store.updates) == 0 || store.updates[0] != nil {
		t.Fatal("chunks were not reset when cache files were missing")
	}
}

func TestOpenCachedFileUsesFinalFileAsComplete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := testMediaSource(ChunkSize + 10)
	itemDir := filepath.Join(dir, source.ItemName)
	if err := os.MkdirAll(itemDir, 0o755); err != nil {
		t.Fatalf("mkdir item dir: %v", err)
	}
	finalPath := filepath.Join(itemDir, source.MediaSourceID+"."+source.Container)
	if err := os.WriteFile(finalPath, make([]byte, source.Size), 0o644); err != nil {
		t.Fatalf("write final file: %v", err)
	}
	store := &fakeChunkStore{chunks: nil}

	file, err := OpenCachedFile(ctx, dir, source, store)
	if err != nil {
		t.Fatalf("open cached file: %v", err)
	}
	defer file.Close(ctx)
	if !file.finalized {
		t.Fatal("final file was not treated as finalized")
	}
	if !file.chunks.Complete() {
		t.Fatal("final file chunks were not marked complete")
	}
}

func TestOpenCachedFileResumesValidProgress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := testMediaSource(ChunkSize * 2)
	itemDir := filepath.Join(dir, source.ItemName)
	if err := os.MkdirAll(itemDir, 0o755); err != nil {
		t.Fatalf("mkdir item dir: %v", err)
	}
	progressPath := filepath.Join(itemDir, source.MediaSourceID+"."+source.Container+".progress")
	if err := os.WriteFile(progressPath, make([]byte, source.Size), 0o644); err != nil {
		t.Fatalf("write progress file: %v", err)
	}
	bitset := NewBitset(chunkCount(source.Size))
	bitset.Set(0, true)
	source.Chunks = bitset.Bytes()
	store := &fakeChunkStore{chunks: source.Chunks}

	file, err := OpenCachedFile(ctx, dir, source, store)
	if err != nil {
		t.Fatalf("open cached file: %v", err)
	}
	defer file.Close(ctx)
	if !file.ChunkComplete(0) {
		t.Fatal("valid progress chunks were not restored")
	}
	if file.ChunkComplete(1) {
		t.Fatal("unexpected chunk restored as complete")
	}
}

func testMediaSource(size int64) store.MediaSource {
	return store.MediaSource{
		MediaSourceID: "media-source",
		ItemID:        "item",
		ItemName:      "item-name",
		Size:          size,
		Container:     "mkv",
		Bitrate:       1,
	}
}

func completeChunks(size int64) []byte {
	bitset := NewBitset(chunkCount(size))
	for i := 0; i < bitset.Size(); i++ {
		bitset.Set(i, true)
	}
	return bitset.Bytes()
}
