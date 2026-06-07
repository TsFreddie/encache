package interceptor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"emby-proxy-cache/internal/cache"

	"github.com/fereidani/httpdecompressor"
)

var downloadPath = regexp.MustCompile(`^/emby/items/([0-9]+)/download/?$`)

const playbackBody = `{"DeviceProfile":{"SubtitleProfiles":[{"Method":"Embed","Format":"ass"},{"Format":"ssa","Method":"Embed"},{"Format":"subrip","Method":"Embed"},{"Format":"sub","Method":"Embed"},{"Method":"Embed","Format":"pgssub"},{"Method":"Embed","Format":"DVDSUB"},{"Method":"Embed","Format":"VOBSUB"},{"Format":"subrip","Method":"External"},{"Method":"External","Format":"sub"},{"Method":"External","Format":"ass"},{"Format":"ssa","Method":"External"},{"Method":"External","Format":"vtt"},{"Method":"External","Format":"ass"},{"Format":"ssa","Method":"External"}],"CodecProfiles":[{"Codec":"h264","Type":"Video","ApplyConditions":[{"Property":"IsAnamorphic","Value":"true","Condition":"NotEquals","IsRequired":false},{"IsRequired":false,"Value":"high|main|baseline|constrained baseline","Condition":"EqualsAny","Property":"VideoProfile"},{"IsRequired":false,"Value":"80","Condition":"LessThanEqual","Property":"VideoLevel"},{"IsRequired":false,"Value":"true","Condition":"NotEquals","Property":"IsInterlaced"}]},{"Codec":"hevc","ApplyConditions":[{"Property":"IsAnamorphic","Value":"true","Condition":"NotEquals","IsRequired":false},{"IsRequired":false,"Value":"high|main|main 10","Condition":"EqualsAny","Property":"VideoProfile"},{"Property":"VideoLevel","Value":"175","Condition":"LessThanEqual","IsRequired":false},{"IsRequired":false,"Value":"true","Condition":"NotEquals","Property":"IsInterlaced"}],"Type":"Video"}],"MaxStreamingBitrate":40000000,"TranscodingProfiles":[{"Container":"ts","AudioCodec":"aac,mp3,wav,ac3,eac3,flac,opus","VideoCodec":"hevc,h264,mpeg4","BreakOnNonKeyFrames":true,"Type":"Video","MaxAudioChannels":"6","Protocol":"hls","Context":"Streaming","MinSegments":2}],"DirectPlayProfiles":[{"Container":"mov,fmp4,mp3,mpegts,flac,3gp,aac,flv,ogg,wav,mp4,mkv,ts,hls,webm,avi,wmv,m4v,m2ts,mts,rm,rmvb,f4v,wma","Type":"Video","VideoCodec":"h263,av1,mpeg4,h264,mpeg1video,mpeg2video,hevc,dvhe,dvh1,h264,hevc,hev1,mpeg4,vp8,vp9,wmv9","AudioCodec":"aac,mp1,alac,mp2,mp4als,mp3,vorbis,wav,ac3,eac3,mlp,flac,truehd,dts,dca,opus,pcm,pcm_s24le,pcm_s8,pcm_s16be,pcm_s16le,pcm_s24le,pcm_s32le,pcm_f32le,pcm_alaw,pcm_mulaw"}],"ResponseProfiles":[{"MimeType":"video/mp4","Type":"Video","Container":"m4v"}],"ContainerProfiles":[],"MusicStreamingTranscodingBitrate":40000000,"MaxStaticBitrate":40000000}}`

type EnableDownload struct {
	Base
	Cache *cache.Manager
}

type playbackInfoResponse struct {
	MediaSources []struct {
		ID              string `json:"Id"`
		DirectStreamURL string `json:"DirectStreamUrl"`
	} `json:"MediaSources"`
}

func (e EnableDownload) OnRequest(ctx *Context) (*http.Response, bool, error) {
	if e.Cache == nil {
		return nil, false, nil
	}
	req := ctx.Request
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return nil, false, nil
	}
	matches := downloadPath.FindStringSubmatch(strings.ToLower(req.URL.Path))
	if matches == nil {
		return nil, false, nil
	}

	rangeHeader := req.Header.Get("Range")
	if rangeHeader == "" {
		rangeHeader = "bytes=0-"
	}
	mediaSourceID := firstQuery(req, "MediaSourceId", "mediasourceid")
	if mediaSourceID == "" {
		return simpleResponse(req, http.StatusNotFound), true, nil
	}
	apiKey := firstQuery(req, "api_key", "ApiKey")
	if apiKey == "" {
		return simpleResponse(req, http.StatusForbidden), true, nil
	}

	handle, err := e.Cache.Open(req.Context(), mediaSourceID)
	if err != nil {
		if err == cache.ErrMediaSourceNotFound {
			return simpleResponse(req, http.StatusNotFound), true, nil
		}
		return nil, false, err
	}

	start, end, err := parseByteRange(rangeHeader, handle.Source.Size)
	if err != nil {
		_ = handle.Close()
		return rangeNotSatisfiable(req, handle.Source.Size), true, nil
	}
	upstreamURL, err := e.resolveDirectStreamURL(req, matches[1], mediaSourceID, apiKey)
	if err != nil {
		_ = handle.Close()
		return nil, false, err
	}

	reader, err := handle.File.ReadRange(req.Context(), start, end, cache.FetchOptions{
		Class:       cache.SessionPassive,
		Request:     req,
		UpstreamURL: upstreamURL,
		Client:      e.Cache.Client,
		Gate:        e.Cache.Gate,
	})
	if err != nil {
		_ = handle.Close()
		return nil, false, err
	}

	body := closeBoth{Reader: reader, CloseFunc: handle.Close}
	if req.Method == http.MethodHead {
		_ = body.Close()
		body = closeBoth{Reader: http.NoBody, CloseFunc: func() error { return nil }}
	}

	response := &http.Response{
		StatusCode:    http.StatusPartialContent,
		Status:        "206 Partial Content",
		Header:        make(http.Header),
		Body:          body,
		ContentLength: end - start + 1,
		Request:       req,
	}
	response.Header.Set("Accept-Ranges", "bytes")
	response.Header.Set("Access-Control-Allow-Headers", "Accept, Accept-Language, Authorization, Cache-Control, Content-Disposition, Content-Encoding, Content-Language, Content-Length, Content-MD5, Content-Range, Content-Type, Date, Host, If-Match, If-Modified-Since, If-None-Match, If-Unmodified-Since, Origin, OriginToken, Pragma, Range, Slug, Transfer-Encoding, Want-Digest, X-MediaBrowser-Token, X-Emby-Token, X-Emby-Client, X-Emby-Client-Version, X-Emby-Device-Id, X-Emby-Device-Name, X-Emby-Authorization")
	response.Header.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
	response.Header.Set("Access-Control-Allow-Origin", "*")
	response.Header.Set("Cache-Control", "private, no-transform")
	response.Header.Set("Content-Length", strconv.FormatInt(response.ContentLength, 10))
	response.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, handle.Source.Size))
	response.Header.Set("Content-Type", "application/octet-stream")

	fmt.Printf("[Download] queued item=%s mediaSource=%s range=%d-%d/%d\n", matches[1], mediaSourceID, start, end, handle.Source.Size)
	return response, true, nil
}

func (e EnableDownload) OnResponse(ctx *Context, response *http.Response) (*http.Response, error) {
	path := ctx.Request.URL.Path
	if !strings.HasPrefix(path, "/emby/Items") && !strings.HasPrefix(path, "/emby/Videos") && !strings.HasPrefix(path, "/emby/Users") {
		return response, nil
	}
	if response.Body == nil || !strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "application/json") {
		return response, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	_ = response.Body.Close()
	decoded, err := decodeDownloadBody(body, response.Header.Get("Content-Encoding"))
	if err != nil {
		response.Body = io.NopCloser(bytes.NewReader(body))
		return response, nil
	}

	var payload any
	if err := json.Unmarshal(decoded, &payload); err != nil {
		response.Body = io.NopCloser(bytes.NewReader(body))
		return response, nil
	}
	setDownloadEnabled(payload)
	updated, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(updated))
	response.ContentLength = int64(len(updated))
	response.Header.Del("Content-Encoding")
	response.Header.Set("Content-Length", strconv.Itoa(len(updated)))
	return response, nil
}

func (e EnableDownload) resolveDirectStreamURL(req *http.Request, itemID, mediaSourceID, apiKey string) (*url.URL, error) {
	requestURL := *e.Cache.UpstreamURL
	requestURL.Path = joinURLPath(e.Cache.UpstreamURL.Path, "/emby/Items/"+itemID+"/PlaybackInfo")
	query := requestURL.Query()
	query.Set("api_key", apiKey)
	query.Set("mediaSourceId", mediaSourceID)
	requestURL.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(req.Context(), http.MethodPost, requestURL.String(), strings.NewReader(playbackBody))
	if err != nil {
		return nil, err
	}
	copyDownloadHeaders(request.Header, req.Header)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Host = requestURL.Host

	client := e.Cache.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("playback info status %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeDownloadBody(body, response.Header.Get("Content-Encoding"))
	if err != nil {
		return nil, err
	}
	var info playbackInfoResponse
	if err := json.Unmarshal(decoded, &info); err != nil {
		return nil, err
	}
	for _, source := range info.MediaSources {
		if source.ID == mediaSourceID && source.DirectStreamURL != "" {
			return url.Parse(joinURLPath(e.Cache.UpstreamURL.String(), source.DirectStreamURL))
		}
	}
	return nil, fmt.Errorf("direct stream url not found for mediaSource=%s", mediaSourceID)
}

func setDownloadEnabled(value any) {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed["CanDownload"]; ok {
			typed["CanDownload"] = true
		}
		if policy, ok := typed["Policy"].(map[string]any); ok {
			policy["EnableContentDownloading"] = true
		}
		for _, child := range typed {
			setDownloadEnabled(child)
		}
	case []any:
		for _, child := range typed {
			setDownloadEnabled(child)
		}
	}
}

func decodeDownloadBody(body []byte, contentEncoding string) ([]byte, error) {
	if strings.TrimSpace(contentEncoding) == "" {
		return body, nil
	}
	reader, err := httpdecompressor.ReaderFromReader(io.NopCloser(bytes.NewReader(body)), strings.ToLower(strings.TrimSpace(contentEncoding)))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func simpleResponse(req *http.Request, status int) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: make(http.Header), Body: http.NoBody, Request: req}
}

func copyDownloadHeaders(dst, src http.Header) {
	for key, values := range src {
		if isDownloadHopHeader(key) || strings.EqualFold(key, "Range") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isDownloadHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "transfer-encoding", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "upgrade":
		return true
	default:
		return false
	}
}

func joinURLPath(base, path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
}
