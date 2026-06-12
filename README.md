# Encache

Encache sits between your Emby clients and your Emby server to make repeated playback smoother. It keeps frequently requested media data close to the player, reducing repeated traffic to the upstream server.

## What It Does

- Works as a proxy in front of Emby.
- Caches media as it is played.
- Reuses cached data for future playback.
- Keeps cache data on disk so it can survive restarts.
- Can optionally help with download workflows.

## When To Use It

Use Encache when your Emby server or network connection benefits from serving repeated playback data locally. It is especially useful when multiple clients replay the same media or when upstream bandwidth is limited.

## Getting Started

Run Encache with Docker, point it at your Emby server, and configure your clients to connect through Encache instead of directly to Emby.

Docker setup instructions are available in [DOCKER.md](DOCKER.md).

Tagged releases include a Docker image and a downloadable Linux binary zip.

## Notes

- Encache should be placed on a trusted network with access to your Emby server.
- Cache storage can grow over time depending on playback volume.
- Persistent storage is recommended so cached data is not lost when the service restarts.
