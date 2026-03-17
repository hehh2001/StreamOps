# StreamOps

StreamOps is a sanitized public reference build of a live monitoring frontend and playback gateway used in RTMP distribution workflows.

It focuses on one thing: giving operators a single watch entry that can adapt playback behavior across desktop and mobile clients while still integrating with an external control plane for stream state and viewer statistics.

## Highlights

- Single watch URL for desktop and mobile monitoring
- HTTP-FLV oriented desktop playback path
- HLS fallback path for mobile clients
- Edge-aware playback routing
- Viewer aggregation across monitor-side web viewers and upstream RTMP consumers
- Lightweight deployment with Python + Nginx

## Repository Layout

- `server.py`: Python HTTP service for watch pages, playback redirects, and viewer aggregation
- `assets/`: bundled `flv.js` and `hls.js`
- `html/watch.html`: legacy static watch page example
- `nginx.conf`: sample reverse proxy for HLS edge distribution
- `docker-compose.yml`: sample Nginx container wiring
- `config.example.yml`: sanitized configuration template

## Sanitization Notes

- Real domains, IP addresses, and upstream hostnames
- Business identifiers, contact details, and compliance labels
- Operator credentials and API passwords
- Runtime logs, Python bytecode, and iterative backup files

## Architecture

This project assumes you already have:

- an ingest/control system that writes stream/job metadata
- one or more edge nodes exposing SRS-style APIs
- a monitor entry domain that fronts this service

At runtime, `server.py` reads:

- playback settings from `config.yml`
- stream job metadata from `runtime/jobs.json` or another path you supply

The monitor page can:

- choose FLV for desktop/Android when appropriate
- request HLS for mobile clients
- fetch edge-backed playlists and segments
- combine monitor viewers with upstream RTMP viewer counts

## Configuration

Copy `config.example.yml` to `config.yml` and edit it:

```bash
cp config.example.yml config.yml
mkdir -p runtime
printf '{}\n' > runtime/jobs.json
```

You can also override paths with environment variables:

- `STREAM_WATCH_CONFIG`
- `STREAM_WATCH_ASSETS_DIR`
- `STREAM_WATCH_JOBS`

## Local Run

Install the only Python dependency:

```bash
python3 -m pip install pyyaml
```

Start the service:

```bash
python3 server.py
```

By default it listens on `127.0.0.1:17070`.

## Reverse Proxy

Use a front proxy to expose:

- `/watch/<stream_name>`
- `/flv/<app>/<stream_name>.flv`
- `/hls/<app>/<stream_name>.m3u8`

The bundled `nginx.conf` is only a sample. Replace the placeholder edge hostnames with your own upstreams before deployment.

## Release Notes

This public repository intentionally does not include:

- the original private control plane project
- private edge topology
- operational credentials
- deployment-specific branding

## License

MIT

## Notes

- This repository is intentionally published without the original control plane code.
- `server.py` expects an external job/state source; adapt `load_jobs()` if your control plane differs.
- The static `html/watch.html` file is a minimal legacy page. The main delivered UI is embedded in `server.py`.
