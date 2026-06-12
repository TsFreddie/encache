# Encache Docker Setup

This project includes a multi-stage `Dockerfile` that builds the Go binary and runs it in a small Alpine-based image as a non-root user.

## Build The Image

From the repository root:

```sh
docker build -t encache:local .
```

Tagged releases are published to GitHub Container Registry as:

```text
ghcr.io/<owner>/<repository>:<tag>
```

## Run With Docker

Run the proxy and persist cache storage in a named volume:

```sh
docker run --rm \
  --name encache \
  -p 3000:3000 \
  -e UPSTREAM_URL=http://host.docker.internal:8096 \
  -v encache-storage:/app/storage \
  encache:local
```

To use a published release, replace `encache:local` with the GHCR image tag for your repository.

On Linux, `host.docker.internal` may not be available by default. If your Emby server runs on the Docker host, add:

```sh
--add-host=host.docker.internal:host-gateway
```

Full Linux example:

```sh
docker run --rm \
  --name encache \
  --add-host=host.docker.internal:host-gateway \
  -p 3000:3000 \
  -e UPSTREAM_URL=http://host.docker.internal:8096 \
  -v encache-storage:/app/storage \
  encache:local
```

## Environment Variables

| Variable | Default In Image | Description |
| --- | --- | --- |
| `UPSTREAM_URL` | `http://localhost:8096` | Base URL of the upstream Emby server. Set this for normal container usage. |
| `HOST` | `0.0.0.0` | Address the proxy binds to inside the container. |
| `PORT` | `3000` | Port the proxy listens on inside the container. |
| `STORAGE_PATH` | `/app/storage` | Container path for cached media chunks and SQLite metadata. |
| `MAX_SESSIONS` | `1` | Maximum playback sessions tracked by the playback event log. |
| `ENABLE_DOWNLOAD` | unset | Set to `1` to enable proxied download handling. |
| `CLEANUP_DAYS` | `0` | Delete cached files older than this many days. `0` disables cleanup. |

## Persistent Storage

Use a volume or bind mount for `/app/storage` so cache files and `metadata.sqlite` survive container restarts.

Named volume:

```sh
-v encache-storage:/app/storage
```

Bind mount:

```sh
-v /path/on/host/encache:/app/storage
```

If you use a bind mount, ensure the container user can write to the directory. The image runs as the non-root `encache` user.

## Docker Compose

Example `compose.yaml`:

```yaml
services:
  encache:
    build: .
    container_name: encache
    ports:
      - "3000:3000"
    environment:
      UPSTREAM_URL: http://host.docker.internal:8096
      CLEANUP_DAYS: "30"
    volumes:
      - encache-storage:/app/storage
    extra_hosts:
      - "host.docker.internal:host-gateway"
    restart: unless-stopped

volumes:
  encache-storage:
```

Start it with:

```sh
docker compose up -d --build
```

View logs:

```sh
docker logs -f encache
```

Stop it:

```sh
docker compose down
```

## Health Check

After the container starts, verify the proxy is responding:

```sh
curl http://localhost:3000/health
```

Expected response:

```json
{"status":"ok"}
```

## Client Setup

Configure Emby clients or any reverse proxy in front of Emby to use the container endpoint instead of the upstream Emby server directly.

Example local proxy URL:

```text
http://localhost:3000
```

The proxy forwards requests to `UPSTREAM_URL` and caches eligible ranged video stream responses in `/app/storage`.

## Troubleshooting

- If the container starts but clients cannot reach Emby, verify `UPSTREAM_URL` is reachable from inside the container.
- If cached data disappears after restart, verify `/app/storage` is mounted to a persistent volume or host directory.
- If bind-mounted storage fails with permission errors, fix ownership or permissions on the host directory.
- If downloads are not handled by the proxy, set `ENABLE_DOWNLOAD=1` and recreate the container.
