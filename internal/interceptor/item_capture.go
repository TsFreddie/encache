package interceptor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"emby-proxy-cache/internal/store"

	"github.com/fereidani/httpdecompressor"
)

var playbackInfoPath = regexp.MustCompile(`^/emby/Items/([0-9]+)/PlaybackInfo/?$`)

type ItemCapture struct {
	Base
	Store *store.Store
}

type embyItem struct {
	MediaSources []embyMediaSource `json:"MediaSources"`
}

type embyMediaSource struct {
	ID        string `json:"Id"`
	ItemID    string `json:"ItemId"`
	Name      string `json:"Name"`
	Path      string `json:"Path"`
	Size      int64  `json:"Size"`
	Container string `json:"Container"`
	Bitrate   int64  `json:"Bitrate"`
}

func (i ItemCapture) OnResponse(ctx *Context, response *http.Response) (*http.Response, error) {
	if i.Store == nil || response.Body == nil {
		return response, nil
	}
	matches := playbackInfoPath.FindStringSubmatch(ctx.Request.URL.Path)
	if matches == nil {
		return response, nil
	}
	itemID := matches[1]
	if !strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "application/json") {
		return response, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	_ = response.Body.Close()

	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))

	decodedBody, err := decodeBodyForInspection(body, response.Header.Get("Content-Encoding"))
	if err != nil {
		fmt.Printf("[ItemCapture] decode failed %s: %v\n", ctx.Request.URL.Path, err)
		return response, nil
	}

	var item embyItem
	if err := json.Unmarshal(decodedBody, &item); err != nil {
		fmt.Printf("[ItemCapture] parse failed %s: %v\n", ctx.Request.URL.Path, err)
		return response, nil
	}
	logInterceptedMediaSources(ctx.Request.URL.Path, item.MediaSources)

	inserted := 0
	for _, mediaSource := range item.MediaSources {
		mediaItemID := mediaSourceItemID(mediaSource, itemID)
		ok, err := i.Store.InsertMediaSource(ctx.Request.Context(), store.MediaSource{
			MediaSourceID: mediaSource.ID,
			ItemID:        mediaItemID,
			ItemName:      store.SanitizeFilename(mediaSourceItemName(mediaSource, mediaItemID)),
			SourceName:    store.SanitizeFilename(mediaSourceName(mediaSource)),
			Size:          mediaSource.Size,
			Container:     mediaSource.Container,
			Bitrate:       mediaSource.Bitrate,
		})
		if err != nil {
			return nil, err
		}
		if ok {
			inserted++
		}
	}

	fmt.Printf(
		"[ItemCapture] item=%s mediaSources=%d inserted=%d\n",
		itemID,
		len(item.MediaSources),
		inserted,
	)
	return response, nil
}

func mediaSourceItemID(mediaSource embyMediaSource, fallback string) string {
	if mediaSource.ItemID != "" {
		return mediaSource.ItemID
	}
	return fallback
}

func mediaSourceItemName(mediaSource embyMediaSource, fallback string) string {
	if mediaSource.Path != "" {
		name := strings.TrimSuffix(filepath.Base(mediaSource.Path), filepath.Ext(mediaSource.Path))
		if name != "" && name != "." {
			return name
		}
	}
	return fallback
}

func mediaSourceName(mediaSource embyMediaSource) string {
	if mediaSource.Name != "" {
		return mediaSource.Name
	}
	if mediaSource.Path != "" {
		return mediaSource.Path
	}
	return mediaSource.ID
}

func decodeBodyForInspection(body []byte, contentEncoding string) ([]byte, error) {
	reader, err := httpdecompressor.ReaderFromReader(
		io.NopCloser(bytes.NewReader(body)),
		strings.ToLower(strings.TrimSpace(contentEncoding)),
	)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func logInterceptedMediaSources(path string, mediaSources []embyMediaSource) {
	fmt.Printf("[ItemCapture] playbackinfo %s mediaSources=%d\n", path, len(mediaSources))
	for _, mediaSource := range mediaSources {
		fmt.Printf(
			"[ItemCapture] source id=%s itemId=%s name=%q container=%s size=%d bitrate=%d path=%q\n",
			mediaSource.ID,
			mediaSource.ItemID,
			mediaSource.Name,
			mediaSource.Container,
			mediaSource.Size,
			mediaSource.Bitrate,
			mediaSource.Path,
		)
	}
}
