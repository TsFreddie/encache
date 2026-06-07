package interceptor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"emby-proxy-cache/internal/store"
)

func TestItemCaptureCapturesUserItemEndpoint(t *testing.T) {
	dir := t.TempDir()
	metadata, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer metadata.Close()

	req := httptest.NewRequest(http.MethodGet, "/emby/Users/f5fe7ad06efe43e683a6c4739055754a/Items/279033", nil)
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(`{
			"Id":"279033",
			"Name":"逃犯搜捕者",
			"MediaSources":[{
				"Bitrate":28664724,
				"Container":"mkv",
				"Id":"mediasource_279033",
				"ItemId":"279033",
				"Name":"神烦警探 - S07E01 - 第1集",
				"Path":"/mount/pro/115open(333190)/video/欧美剧/神烦警探 (2013)/Season 7/神烦警探 - S07E01 - 第1集.mkv",
				"Size":4626221310
			}]
		}`)),
	}
	response.Header.Set("Content-Type", "application/json")

	_, err = (ItemCapture{Store: metadata}).OnResponse(&Context{Request: req}, response)
	if err != nil {
		t.Fatalf("capture response: %v", err)
	}

	source, ok, err := metadata.GetMediaSource(context.Background(), "mediasource_279033")
	if err != nil {
		t.Fatalf("get media source: %v", err)
	}
	if !ok {
		t.Fatal("media source was not captured")
	}
	if source.ItemID != "279033" || source.Container != "mkv" || source.Size != 4626221310 || source.Bitrate != 28664724 {
		t.Fatalf("unexpected source: %+v", source)
	}
}
