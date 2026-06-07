package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"emby-proxy-cache/internal/cache"
	"emby-proxy-cache/internal/config"
	"emby-proxy-cache/internal/interceptor"
	"emby-proxy-cache/internal/proxy"
	"emby-proxy-cache/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := os.MkdirAll(cfg.StoragePath, 0o755); err != nil {
		log.Fatalf("create storage path: %v", err)
	}

	store, err := store.Open(context.Background(), cfg.StoragePath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	cacheManager := cache.NewManager(cfg.StoragePath, cfg.UpstreamURL, store)
	playbackEventLog := &interceptor.PlaybackEventLog{MaxSessions: cfg.MaxSessions}
	chain := []interceptor.Interceptor{}
	if cfg.EnableDownload {
		chain = append(chain, interceptor.EnableDownload{Cache: cacheManager})
	}
	chain = append(chain,
		interceptor.StreamCache{Cache: cacheManager},
		playbackEventLog,
		interceptor.ItemCapture{Store: store},
		interceptor.Logger{},
	)

	handler := proxy.New(cfg.UpstreamURL, chain)
	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("Emby Proxy running on http://%s:%d", cfg.Host, cfg.Port)
	log.Printf("Upstream: %s", cfg.UpstreamURL.String())
	log.Printf("Storage: %s", cfg.StoragePath)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
