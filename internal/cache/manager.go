package cache

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"encache/internal/store"
	"encache/internal/upstream"
)

type Store interface {
	GetMediaSource(ctx context.Context, mediaSourceID string) (store.MediaSource, bool, error)
	GetPreferredMediaSourceByItemID(ctx context.Context, itemID string) (store.MediaSource, bool, error)
	UpdateChunks(ctx context.Context, mediaSourceID string, chunks []byte) error
}

type Manager struct {
	Store       Store
	StoragePath string
	Client      *http.Client
	UpstreamURL *url.URL
	Upstream    *upstream.Upstream
	Gate        *DownloadGate

	mu    sync.Mutex
	files map[string]*openFile

	cleanupDone chan struct{}
	opening     int
	openingIdle chan struct{}
}

type openFile struct {
	refs int
	file *CachedFile
}

type Handle struct {
	Source store.MediaSource
	File   *CachedFile
	done   func() error
}

func NewManager(storagePath string, upstreamURL, fallbackUpstreamURL *url.URL, client *http.Client, store Store, fallbackDuration time.Duration) *Manager {
	if client == nil {
		client = upstream.NewClient()
	}
	return &Manager{
		Store:       store,
		StoragePath: storagePath,
		Client:      client,
		UpstreamURL: upstreamURL,
		Upstream:    upstream.New(upstreamURL, fallbackUpstreamURL, client, fallbackDuration),
		Gate:        NewDownloadGate(),
		files:       make(map[string]*openFile),
	}
}

func (m *Manager) Open(ctx context.Context, mediaSourceID string) (*Handle, error) {
	return m.open(ctx, mediaSourceID, func() (string, store.MediaSource, bool, error) {
		source, ok, err := m.Store.GetMediaSource(ctx, mediaSourceID)
		return mediaSourceID, source, ok, err
	})
}

func (m *Manager) OpenPreferredByItemID(ctx context.Context, itemID string) (*Handle, error) {
	return m.open(ctx, "", func() (string, store.MediaSource, bool, error) {
		source, ok, err := m.Store.GetPreferredMediaSourceByItemID(ctx, itemID)
		return source.MediaSourceID, source, ok, err
	})
}

func (m *Manager) open(ctx context.Context, initialKey string, load func() (string, store.MediaSource, bool, error)) (*Handle, error) {
	if err := m.beginOpen(ctx); err != nil {
		return nil, err
	}
	defer m.endOpen()

	if initialKey != "" {
		m.mu.Lock()
		if item, ok := m.files[initialKey]; ok {
			item.refs++
			handle := &Handle{Source: item.file.Source(), File: item.file}
			handle.done = func() error { return m.release(context.Background(), initialKey) }
			m.mu.Unlock()
			return handle, nil
		}
		m.mu.Unlock()
	}

	key, source, ok, err := load()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrMediaSourceNotFound
	}
	if key == "" {
		key = source.MediaSourceID
	}

	m.mu.Lock()
	if item, ok := m.files[key]; ok {
		item.refs++
		handle := &Handle{Source: item.file.Source(), File: item.file}
		handle.done = func() error { return m.release(context.Background(), key) }
		m.mu.Unlock()
		return handle, nil
	}
	m.mu.Unlock()

	file, err := OpenCachedFile(ctx, m.StoragePath, source, m.Store)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if item, ok := m.files[key]; ok {
		item.refs++
		m.mu.Unlock()
		_ = file.Close(ctx)
		handle := &Handle{Source: item.file.Source(), File: item.file}
		handle.done = func() error { return m.release(context.Background(), key) }
		return handle, nil
	}
	m.files[key] = &openFile{refs: 1, file: file}
	m.mu.Unlock()

	handle := &Handle{Source: source, File: file}
	handle.done = func() error { return m.release(context.Background(), key) }
	return handle, nil
}

func (m *Manager) StartDailyCleanup(ctx context.Context, retentionDays int) {
	if retentionDays <= 0 {
		return
	}
	go func() {
		m.cleanupOldFiles(ctx, retentionDays)

		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.cleanupOldFiles(ctx, retentionDays)
			}
		}
	}()
}

func (m *Manager) cleanupOldFiles(ctx context.Context, retentionDays int) {
	if retentionDays <= 0 {
		return
	}
	openPaths, finish, err := m.beginCleanup(ctx)
	if err != nil {
		return
	}
	defer finish()

	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	if err := filepath.WalkDir(m.StoragePath, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			fmt.Printf("[CacheCleanup] skip path=%q err=%v\n", path, err)
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if skipCleanupPath(path) || openPaths[path] {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			fmt.Printf("[CacheCleanup] stat path=%q err=%v\n", path, err)
			return nil
		}
		if info.ModTime().After(cutoff) {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			fmt.Printf("[CacheCleanup] remove path=%q err=%v\n", path, err)
		}
		return nil
	}); err != nil {
		fmt.Printf("[CacheCleanup] walk storage=%q err=%v\n", m.StoragePath, err)
	}
}

func (m *Manager) beginOpen(ctx context.Context) error {
	for {
		m.mu.Lock()
		if m.cleanupDone == nil {
			m.opening++
			m.mu.Unlock()
			return nil
		}
		done := m.cleanupDone
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}
}

func (m *Manager) endOpen() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.opening--
	if m.opening == 0 && m.openingIdle != nil {
		close(m.openingIdle)
		m.openingIdle = nil
	}
}

func (m *Manager) beginCleanup(ctx context.Context) (map[string]bool, func(), error) {
	for {
		m.mu.Lock()
		if m.cleanupDone == nil {
			m.cleanupDone = make(chan struct{})
			break
		}
		done := m.cleanupDone
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-done:
		}
	}

	for m.opening > 0 {
		if m.openingIdle == nil {
			m.openingIdle = make(chan struct{})
		}
		idle := m.openingIdle
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			m.finishCleanup()
			return nil, nil, ctx.Err()
		case <-idle:
		}

		m.mu.Lock()
	}

	openPaths := make(map[string]bool, len(m.files)*2)
	for _, item := range m.files {
		openPaths[item.file.path] = true
		openPaths[item.file.progressPath] = true
	}
	m.mu.Unlock()

	return openPaths, m.finishCleanup, nil
}

func (m *Manager) finishCleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cleanupDone != nil {
		close(m.cleanupDone)
		m.cleanupDone = nil
	}
}

func skipCleanupPath(path string) bool {
	base := filepath.Base(path)
	return base == "metadata.sqlite" || base == "metadata.sqlite-wal" || base == "metadata.sqlite-shm"
}

func (h *Handle) Close() error {
	if h.done == nil {
		return nil
	}
	done := h.done
	h.done = nil
	return done()
}

func (m *Manager) release(ctx context.Context, mediaSourceID string) error {
	m.mu.Lock()
	item, ok := m.files[mediaSourceID]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	item.refs--
	if item.refs > 0 {
		m.mu.Unlock()
		return nil
	}
	delete(m.files, mediaSourceID)
	m.mu.Unlock()
	return item.file.Close(ctx)
}
