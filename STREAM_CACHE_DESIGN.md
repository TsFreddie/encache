# Stream Cache Design

## Goal

Build a real file-backed streaming cache for Emby video playback. The cache should serve byte-range video requests from disk when possible, fetch missing chunks from upstream on demand, persist progress across restarts, and finalize complete media files into stable storage paths.

The old TypeScript implementation had the right rough shape, but it stopped before actually caching streams end to end. It detected stream/download routes, had a chunked progress-file model, and started upstream fetches for missing chunks. The Go implementation should keep the useful ideas while separating responsibilities more clearly.

## High-Level Architecture

The system should be split into four layers:

1. `ItemCapture`: captures PlaybackInfo metadata and stores media source rows.
2. `cache.Manager`: owns cache files, chunk maps, locks, reference counts, and persistence.
3. `cache.Fetcher`: performs upstream byte-range reads and writes missing chunks.
4. `StreamCache` interceptor: detects cacheable stream requests and returns cache-backed HTTP responses.

The normal proxy path should remain the fallback for unsupported or failed cache cases.

## Session Classes

Keep the old active/passive distinction, but use it as scheduling policy around the shared cache machinery rather than as two separate caching implementations.

Session classes:

- `active`: user playback. These requests are latency-sensitive and should have priority.
- `passive`: download or background fill. These requests are useful but interruptible.

Both session classes read and write the same cache files, chunk maps, and SQLite metadata. The difference is how upstream fetch work is scheduled and canceled.

Active sessions should:

- Reserve from the active stream connection budget.
- Preempt or pause passive fetches when upstream connection slots are scarce.
- Start fetching missing chunks immediately for the playback range.
- Avoid being canceled solely because passive work is running.

Passive sessions should:

- Use only spare upstream connection capacity.
- Be cancelable when active sessions need slots.
- Resume later from the same persisted chunk bitset.
- Prefer sequential fill from the requested/download offset.

The cache reader itself should not care whether bytes are requested by active playback or passive download. It should ask a scheduler/fetcher for missing chunks with a priority class.

Suggested types:

```go
type SessionClass string

const (
    SessionActive  SessionClass = "active"
    SessionPassive SessionClass = "passive"
)

type FetchRequest struct {
    MediaSourceID string
    StartChunk    int
    Class         SessionClass
    Request       *http.Request
}
```

Connection limits should be global policy on the cache manager or fetch scheduler, not hard-coded into individual readers. Use one total upstream fetch limit:

```go
type SchedulerConfig struct {
    MaxFetches int
}
```

Initial defaults could be conservative:

```go
MaxFetches = 4
```

Passive fetches may use any available slot, but they must make way for active fetches. If all fetch slots are occupied and an active fetch is waiting, the scheduler should cancel or pause one passive fetch so the active fetch can start. Active fetches should not be rejected solely because passive work is running.

## Intercepted Routes

Primary stream routes:

```text
/emby/videos/<itemId>/stream.<container>
/emby/videos/<itemId>/original.<container>
```

Cache only when all of these are true:

- Request method is `GET` or `HEAD`.
- Request has a usable `Range` header, or the interceptor can safely synthesize `bytes=0-`.
- A `MediaSourceId` can be resolved from the request.
- The media source exists in SQLite.
- Media size is known and positive.
- Container is not `m3u8` or `hls`.
- Upstream supports byte ranges.

Media source ID lookup order:

1. `MediaSourceId` query parameter.
2. `mediaSourceId` query parameter.
3. Future option: stored direct stream metadata from PlaybackInfo.

Requests without a known media source ID should pass through to upstream.

## Storage Layout

Use the configured `STORAGE_PATH`:

```text
storage/
  metadata.sqlite
  <itemName>/
    <sourceName>.<container>
    <sourceName>.<container>.progress
```

The `.progress` file is preallocated to the full media size and written sparsely by chunk offset. When all chunks are complete, it is renamed to the final file path.

## Chunk Model

Use fixed-size chunks. The old implementation used 1 MiB. A better first Go default is:

```go
const ChunkSize = 4 * 1024 * 1024
```

Chunk math:

```text
chunkIndex = byteOffset / ChunkSize
chunkStart = chunkIndex * ChunkSize
chunkEnd = min(chunkStart + ChunkSize, mediaSize) - 1
```

Persist completed chunks in `media_sources.chunks` as a compact bitset:

- Header stores chunk count/version.
- Remaining bytes store one bit per completed chunk.
- Update periodically, on close, and on finalization.

## Cache Package Shape

Suggested package layout:

```text
internal/cache/
  bitset.go
  file.go
  manager.go
  fetcher.go
  response.go
```

Core types:

```go
type Manager struct {
    Store       *store.Store
    StoragePath string
    Client      *http.Client
    UpstreamURL *url.URL
}

func (m *Manager) Open(ctx context.Context, mediaSourceID string) (*Handle, error)

type Handle struct {
    Source store.MediaSource
    File   *CachedFile
    Done   func() error
}

type CachedFile struct {
    mu       sync.Mutex
    cond     *sync.Cond
    path     string
    progress string
    file     *os.File
    source   store.MediaSource
    chunks   *Bitset
    pending  map[int]*chunkFetch
}
```

Manager responsibilities:

- Keep one in-process owner per media source.
- Reference count open handles.
- Open existing final files or progress files.
- Create and preallocate new progress files.
- Load and validate chunk bitsets from SQLite.
- Persist chunk bitsets.
- Rename complete progress files to final files.

## Reading Behavior

Expose a range reader:

```go
func (f *CachedFile) ReadRange(
    ctx context.Context,
    start int64,
    end int64,
    fetch FetchFunc,
) (io.ReadCloser, error)
```

For each needed chunk:

- If complete, read from disk with `ReadAt`.
- If missing and no fetch exists, claim the chunk and start upstream fetching.
- If missing and a fetch exists, wait for that fetch.
- When the chunk completes, serve bytes from disk or from the completed buffer.
- If fetch fails before any response bytes are sent, the interceptor can fall back to upstream. If it fails mid-response, close the stream with an error and log it.

Only one goroutine may fetch a given chunk. Many readers may wait for the same chunk.

## Fetcher Behavior

When a missing chunk is requested, fetch upstream starting from that chunk boundary:

```text
Range: bytes=<chunkStart>-
```

The fetcher streams upstream bytes into consecutive chunks until one of these occurs:

- Client context is canceled.
- Upstream response ends.
- File completes.
- A fetch/write error occurs.
- A future prefetch policy stops it.

Validation:

- Prefer upstream `206 Partial Content`.
- Validate `Content-Range` total size against metadata.
- Accept upstream `200 OK` only for `bytes=0-` if the length matches the expected media size.
- If upstream size differs from the stored media source size, abort cache use and pass through when possible.

Initial prefetch policy should be conservative: fetch sequentially from the first missing requested chunk and stop when the client disconnects. Later, allow background prefetch for a bounded number of chunks.

Fetch scheduling should account for session class:

- Active fetches enter the high-priority queue.
- Passive fetches enter the low-priority queue.
- The scheduler enforces a single `MaxFetches` limit across both classes.
- If an active fetch needs a slot and all slots are occupied, cancel one passive fetch at a chunk boundary or via context cancellation.
- Canceled passive readers should either block until capacity returns or return a clean client-side cancellation depending on the route behavior.
- Completed chunks from passive work remain valid and immediately benefit active playback.

## HTTP Response Behavior

The `StreamCache` interceptor should build range responses directly.

For valid ranges:

```text
Status: 206 Partial Content
Accept-Ranges: bytes
Content-Type: video/<container> or application/octet-stream
Content-Length: <end-start+1>
Content-Range: bytes <start>-<end>/<total>
Cache-Control: private, no-transform
```

For open-ended ranges:

```text
Range: bytes=<start>-
end = mediaSource.Size - 1
```

For invalid ranges:

```text
Status: 416 Range Not Satisfiable
Content-Range: bytes */<total>
```

For no range, the first implementation should pass through. Full-file `200 OK` cache serving can be added later after range handling is stable.

## Request Flow

1. `StreamCache.OnRequest` checks the route and method.
2. Resolve `MediaSourceId`.
3. Load media source metadata from SQLite.
4. Parse and validate `Range`.
5. Open the cache handle through `cache.Manager`.
6. Build a cache reader for the requested byte range.
7. Return a `206` response with the cache reader as the body.
8. Reader serves completed chunks from disk and waits for missing chunks.
9. Fetcher writes missing chunks into `.progress`.
10. Chunk bitset is persisted periodically and on close.
11. Fully complete progress file is renamed into final file path.

Suggested interceptor order:

```go
chain := []interceptor.Interceptor{
    interceptor.StreamCache{Cache: cacheManager},
    interceptor.ItemCapture{Store: store},
    interceptor.Logger{},
}
```

`StreamCache` must run before normal upstream forwarding because it handles eligible requests in `OnRequest`.

## Concurrency Model

Use per-media-source synchronization.

State:

- `Manager.files map[string]*openFile`
- `openFile.refCount`
- `CachedFile.pending map[int]*chunkFetch`
- `chunkFetch.done chan struct{}`
- `chunkFetch.err error`

Chunk access flow:

1. Lock cached file.
2. If chunk is complete, unlock and read from disk.
3. If chunk has a pending fetch, wait for `done`.
4. If chunk is missing and unclaimed, create pending fetch and start it.
5. Fetcher writes chunk, marks bit complete, closes `done`.

This prevents duplicate upstream requests for the same chunk while allowing many readers to share cached or in-progress data.

## Recovery

On cache open:

- If final file exists and size matches metadata, treat it as complete.
- If `.progress` exists and DB chunk bitset is valid, resume from that bitset.
- If `.progress` size differs from metadata, discard it and recreate.
- If DB chunks are missing but `.progress` exists, discard progress in the first version rather than trusting unknown bytes.
- If media source size changes, discard old progress or treat it as a new cache generation.

Future metadata fields could improve this:

```sql
cache_state TEXT DEFAULT 'partial'
completed_at TEXT
last_accessed_at TEXT
```

For the first implementation, existing `chunks`, `created_at`, and `updated_at` are enough.

## Store Additions

Existing methods are enough for the first implementation:

```go
GetMediaSource(ctx, mediaSourceID)
UpdateChunks(ctx, mediaSourceID, chunks)
```

Useful future addition:

```go
TouchMediaSource(ctx, mediaSourceID)
```

This can support cache cleanup and last-access tracking later.

## What Not To Port Directly

Do not directly port these old implementation details:

- Passive download session behavior as the first target.
- Debug corruption of the bitset.
- Promise-style pending chunk mechanics.
- Full PlaybackInfo body dumps in normal operation.

The Go version should use per-chunk channels or condition variables, structured logs, and explicit response construction.

## Implementation Order

1. Add `internal/cache/bitset.go` with tests.
2. Add `internal/cache/file.go` with progress-file open/read/write/finalize behavior and tests.
3. Add `internal/cache/manager.go` for ref-counted media source ownership.
4. Add `internal/cache/fetcher.go` for upstream range fetches and content-range validation.
5. Add `interceptor.StreamCache` to serve cache-backed `206` responses.
6. Wire `StreamCache` in `main.go` before `ItemCapture` and `Logger`.
7. Add download route support as a second pass after watch-stream caching works.

## Initial Scope

Implement watching-stream cache first:

```text
/emby/videos/<id>/stream.*
/emby/videos/<id>/original.*
```

Then add download support:

```text
/emby/Items/<itemId>/Download
```

This keeps the hard cache mechanics focused on the real playback path before adding passive/fake download session behavior.

When download support is added, downloads should be modeled as `passive` sessions. They should share the same cache files, but their upstream fetches should be lower priority and cancelable so active playback remains smooth.
