package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"encache/internal/cache"
	"encache/internal/config"
	"encache/internal/interceptor"
	"encache/internal/proxy"
	"encache/internal/store"
	"encache/internal/upstream"
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

	upstreamClient := upstream.NewClient()
	sharedUpstream := upstream.New(cfg.UpstreamURL, cfg.FallbackUpstreamURL, upstreamClient, cfg.FallbackDuration)
	cacheManager := cache.NewManager(cfg.StoragePath, cfg.UpstreamURL, cfg.FallbackUpstreamURL, upstreamClient, store, cfg.FallbackDuration)
	cacheManager.StartDailyCleanup(context.Background(), cfg.CleanupDays)
	cacheManager.Upstream = sharedUpstream
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

	handler := proxy.NewWithUpstream(sharedUpstream, chain)
	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("Emby Proxy running on http://%s:%d", cfg.Host, cfg.Port)
	log.Printf("Upstream: %s", cfg.UpstreamURL.String())
	if cfg.FallbackUpstreamURL != nil {
		log.Printf("Fallback: %s", cfg.FallbackUpstreamURL.String())
	}
	log.Printf("Storage: %s", cfg.StoragePath)
	if cfg.CleanupDays > 0 {
		log.Printf("Cleanup: deleting files older than %d days", cfg.CleanupDays)
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
