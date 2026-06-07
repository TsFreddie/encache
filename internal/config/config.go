package config

import (
	"net/url"
	"os"
	"strconv"
)

type Config struct {
	UpstreamURL    *url.URL
	Host           string
	Port           int
	StoragePath    string
	MaxSessions    int
	EnableDownload bool
}

func Load() (Config, error) {
	upstream := getenv("UPSTREAM_URL", "http://localhost:8096")
	upstreamURL, err := url.Parse(upstream)
	if err != nil {
		return Config{}, err
	}

	port, err := strconv.Atoi(getenv("PORT", "3000"))
	if err != nil {
		return Config{}, err
	}
	maxSessions, err := strconv.Atoi(getenv("MAX_SESSIONS", "1"))
	if err != nil {
		return Config{}, err
	}

	return Config{
		UpstreamURL:    upstreamURL,
		Host:           getenv("HOST", "0.0.0.0"),
		Port:           port,
		StoragePath:    getenv("STORAGE_PATH", "./storage"),
		MaxSessions:    maxSessions,
		EnableDownload: os.Getenv("ENABLE_DOWNLOAD") == "1",
	}, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
