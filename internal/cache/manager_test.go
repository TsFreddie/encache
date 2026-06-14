package cache

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"encache/internal/store"
)

type fakeManagerStore struct {
	mu     sync.Mutex
	source store.MediaSource
	gets   int
}

func (s *fakeManagerStore) GetMediaSource(context.Context, string) (store.MediaSource, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	return s.source, true, nil
}

func (s *fakeManagerStore) GetPreferredMediaSourceByItemID(context.Context, string) (store.MediaSource, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	return s.source, true, nil
}

func (s *fakeManagerStore) UpdateChunks(context.Context, string, []byte) error {
	return nil
}

func (s *fakeManagerStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func TestOpenPreferredByItemIDUsesMediaSourceKey(t *testing.T) {
	source := testMediaSource(ChunkSize)
	source.MediaSourceID = "media-source-id"
	store := &fakeManagerStore{source: source}
	manager := NewManager(t.TempDir(), mustURL(t, "http://upstream"), nil, nil, store, 0)

	first, err := manager.OpenPreferredByItemID(context.Background(), source.ItemID)
	if err != nil {
		t.Fatalf("open preferred: %v", err)
	}
	defer first.Close()

	second, err := manager.Open(context.Background(), source.MediaSourceID)
	if err != nil {
		t.Fatalf("open media source: %v", err)
	}
	defer second.Close()

	if first.File != second.File {
		t.Fatal("preferred item open and media source open did not share cached file")
	}
	if store.getCount() != 1 {
		t.Fatalf("store gets = %d, want 1", store.getCount())
	}
}

func TestCleanupOldFilesDeletesOldFilesExceptOpenFiles(t *testing.T) {
	ctx := context.Background()
	storagePath := t.TempDir()
	source := testMediaSource(ChunkSize)
	store := &fakeManagerStore{source: source}
	manager := NewManager(storagePath, mustURL(t, "http://upstream"), nil, nil, store, 0)

	handle, err := manager.Open(ctx, source.MediaSourceID)
	if err != nil {
		t.Fatalf("open media source: %v", err)
	}
	defer handle.Close()

	oldTime := time.Now().AddDate(0, 0, -2)
	if err := os.Chtimes(handle.File.progressPath, oldTime, oldTime); err != nil {
		t.Fatalf("age open progress file: %v", err)
	}

	oldFile := filepath.Join(storagePath, "old-item", "old-source.mkv")
	writeTestFile(t, oldFile, oldTime)
	recentFile := filepath.Join(storagePath, "recent-item", "recent-source.mkv")
	writeTestFile(t, recentFile, time.Now())
	metadataFile := filepath.Join(storagePath, "metadata.sqlite")
	writeTestFile(t, metadataFile, oldTime)

	manager.cleanupOldFiles(ctx, 1)

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("old file still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(handle.File.progressPath); err != nil {
		t.Fatalf("open progress file was removed: %v", err)
	}
	if _, err := os.Stat(recentFile); err != nil {
		t.Fatalf("recent file was removed: %v", err)
	}
	if _, err := os.Stat(metadataFile); err != nil {
		t.Fatalf("metadata file was removed: %v", err)
	}
}

func TestOpenWaitsForCleanup(t *testing.T) {
	storagePath := t.TempDir()
	source := testMediaSource(ChunkSize)
	store := &fakeManagerStore{source: source}
	manager := NewManager(storagePath, mustURL(t, "http://upstream"), nil, nil, store, 0)

	manager.mu.Lock()
	manager.cleanupDone = make(chan struct{})
	manager.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		handle, err := manager.Open(context.Background(), source.MediaSourceID)
		if err == nil {
			err = handle.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("open completed before cleanup ended: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if store.getCount() != 0 {
		t.Fatal("store was loaded before cleanup ended")
	}

	manager.finishCleanup()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("open after cleanup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("open did not resume after cleanup ended")
	}
}

func writeTestFile(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir test file parent: %v", err)
	}
	if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("age test file: %v", err)
	}
}
