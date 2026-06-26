# umans-throttle

A throttling reverse proxy for the [Umans Code](https://app.umans.ai/offers/code/docs) API.

Umans enforces a concurrency limit (Code Max: 4 in-flight requests). Exceeding
it returns HTTP 429, and accumulating >10 concurrency 429s in a day triggers a
5-hour account pause. This proxy caps concurrent upstream requests with a
semaphore, queues overflow with a configurable max wait, and streams responses
back transparently — so you never hit a 429.

```
Agent → [localhost:8080 proxy] → queue/permit → [api.code.umans.ai]
```

## Why

The threat is **daily cumulative** 429s, not single bursts. Backoff-after-429
is too late — you're already counting toward the pause. This proxy prevents
over-limit in-flight at the source.

## Features

- **Concurrency cap** — semaphore limits in-flight upstream requests to your plan's soft cap
- **Queue with timeout** — overflow requests queue up to `max_wait`, then get `503 + Retry-After`
- **Transparent streaming** — full SSE passthrough, zero buffering, protocol-agnostic
- **Idle safety net** — a hung upstream is aborted after `idle_timeout`, releasing the slot
- **RAII permit lifecycle** — the permit is released on every path: completion, client disconnect, error, idle timeout
- **Single binary, ~low memory** — Rust + tokio + axum + reqwest
- **Graceful shutdown** — SIGTERM drains in-flight streams up to `shutdown_timeout`, then forces exit; no new connections accepted during drain
- **Zero per-chunk allocation** — the idle timer is created once per stream and reused via `reset()`, not re-allocated on every gap between SSE chunks

## Quick Start

```bash
# Build
cargo build --release

# Configure
cp config.example.toml config.toml
# edit config.toml: set max_in_flight to your plan (4 = Code Max, 3 = Code Pro)

# Run
./target/release/umans-throttle --config config.toml
```

Point your agent at the proxy instead of Umans directly:

```bash
# Claude Code
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
export ANTHROPIC_AUTH_TOKEN=sk-your-umans-api-key
claude --model umans-coder

# Or any tool that lets you set a base URL + API key
```

The proxy forwards `x-api-key` / `Authorization` headers as-is — it stores no keys.

## Deployment

### Local (manual)

```bash
cargo build --release
cp config.example.toml config.toml  # edit max_in_flight
./target/release/umans-throttle --config config.toml
```

### Server (systemd)

For a persistent service on a remote server (multiple agents connect to it):

```bash
# See deploy/README.md for full guide
sudo cp deploy/umans-throttle.service /etc/systemd/system/
sudo systemctl enable --now umans-throttle
```

Server config needs `listen = "0.0.0.0:8080"` (bind to all interfaces).
Full setup: binary install, systemd, firewall, SSH tunnel — see
[`deploy/README.md`](deploy/README.md).

## Configuration

```toml
[upstream]
base_url = "https://api.code.umans.ai"

[server]
listen = "127.0.0.1:8080"
shutdown_timeout = "30s"  # drain in-flight after SIGTERM, then force exit

[throttle]
max_in_flight = 4      # Code Max=4, Code Pro=3
max_wait = "5m"        # queue timeout -> 503 + Retry-After
idle_timeout = "2m"    # no-bytes -> abort (safety net)
```

Durations use [humantime](https://docs.rs/humantime) format: `"5m"`, `"30s"`, `"2m"`, `"1h"`.

## How It Works

### Semaphore-as-queue

`tokio::sync::Semaphore(max_in_flight)` is both the concurrency counter and the
wait queue. `acquire_owned()` blocks until a slot frees; wrapped in
`timeout(max_wait)` it gives queue-or-reject semantics. No custom queue data
structure — the semaphore's internal FIFO waiter list *is* the queue.

### Permit lifecycle (the core invariant)

The permit is held for the entire duration the upstream request is in-flight:

```
acquire permit → send to upstream → stream response body → [stream ends] → release permit
```

RAII via `OwnedSemaphorePermit`: the permit moves into a custom `PermitBody`
that wraps the response stream. The permit is released when `PermitBody` is
dropped, which happens on:
- **Natural stream end** (end-of-body)
- **Client disconnect** (axum drops the response body)
- **Idle timeout** (stream aborted after `idle_timeout` with no data)
- **Upstream error** (stream ends)

This guarantees the permit is released on **every** path — no leak possible.

```
┌─────────┐     ┌──────────────────────────────────┐     ┌─────────┐
│  Agent  │────▶│  proxy_handler                   │────▶│ Umans   │
│         │     │   acquire permit (queue ≤max_wait)│     │  API    │
│         │     │   forward request (stream)        │     │         │
│         │◀────│   stream response (PermitBody)    │◀────│         │
│         │     │   [stream ends / drop] → release  │     │         │
└─────────┘     └──────────────────────────────────┘     └─────────┘
```

### Protocol-agnostic passthrough

All paths, methods, headers, and bodies are forwarded as-is. The proxy does
not parse request bodies or distinguish `/v1/messages` from
`/v1/chat/completions`. Hop-by-hop headers (RFC 7230 §6.1) are stripped; auth
headers pass through untouched.

### Error responses

| Condition | Status | Body |
|---|---|---|
| Queue timeout | 503 + `Retry-After` | `throttle: queue timeout` |
| Upstream failure | 502 | `throttle: upstream error` |
| Idle timeout | stream ends | (logged; client sees truncated SSE) |

Never returns 429 — that's the error this proxy exists to prevent.

## Architecture

```
src/
├── main.rs     # entry: config, tracing, serve, graceful shutdown
├── lib.rs      # re-exports for integration tests
├── config.rs   # TOML config with humantime durations
└── proxy.rs    # AppState, proxy_handler, PermitBody, header filtering
```

**Dependencies:** axum 0.8 (server) · reqwest 0.12 (client, rustls) · tokio
(runtime, Semaphore) · serde/toml (config) · tracing (logs).

## Development

```bash
cargo build
cargo test           # 6 integration tests
cargo clippy --all-targets
```

Tests use a mock upstream (axum) with configurable hold/release and streaming,
plus the real proxy instance — verifying concurrency limits, queue timeouts,
permit release on completion + client disconnect + idle timeout, header
passthrough, and SSE streaming.

## License

MIT
