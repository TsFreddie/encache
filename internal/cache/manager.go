package cache

import (
	"context"
	"net/http"
	"net/url"
	"sync"

	"emby-proxy-cache/internal/store"
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
	Gate        *DownloadGate

	mu    sync.Mutex
	files map[string]*openFile
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

func NewManager(storagePath string, upstreamURL *url.URL, store Store) *Manager {
	return &Manager{
		Store:       store,
		StoragePath: storagePath,
		Client:      http.DefaultClient,
		UpstreamURL: upstreamURL,
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
