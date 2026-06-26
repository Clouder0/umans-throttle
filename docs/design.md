# umans-throttle — Design

## Problem

Umans Code API enforces a concurrency limit (Code Max: 4 in-flight). Exceeding it
returns HTTP 429. Accumulating >10 concurrency 429s in a day triggers a 5-hour
account pause. The threat is **daily cumulative** 429s, not single bursts — so the
fix must prevent over-limit in-flight at the source, not rely on backoff-after-429.

## Solution

A throttling reverse proxy that sits between the coding agent and the Umans API.
It caps concurrent upstream requests with a semaphore, queues overflow with a
configurable max wait, and streams responses back transparently.

```
Agent → [localhost:8080 proxy] → queue/permit → [api.code.umans.ai]
```

## Core Mechanism: Semaphore-as-Queue

`tokio::sync::Semaphore(max_in_flight)` is both the concurrency counter and the
wait queue. `acquire_owned()` blocks until a slot frees; wrapped in
`timeout(max_wait)` it gives queue-or-reject semantics. No custom queue data
structure needed — the semaphore's internal FIFO waiter list IS the queue.

## Permit Lifecycle (the one hard part)

The permit must be held for the entire duration the upstream request is in-flight:
from the moment we send to upstream until the response body stream ends.

```
acquire permit → send request to upstream → stream response body → [stream ends] → release permit
```

RAII via `OwnedSemaphorePermit`: the permit is moved into a custom `PermitBody`
that wraps the response stream. The permit is released when `PermitBody` is
dropped, which happens on:
- Natural stream end (end-of-body)
- Client disconnect (axum drops the response body)
- Idle timeout (stream aborted)
- Upstream error (stream ends)

This guarantees the permit is released on **every** path — no leak possible.

## PermitBody

Custom `http_body::Body` implementation that:
1. Wraps `reqwest::Response::bytes_stream()` (zero-copy forwarding)
2. Owns the `OwnedSemaphorePermit` (released on drop)
3. Implements idle timeout: if no bytes arrive within `idle_timeout`, the stream
   is aborted (returns `Poll::Ready(None)`). This prevents a hung upstream from
   permanently occupying a slot.

`poll_frame` races the inner stream against a `tokio::time::Sleep`. The sleep
starts when the stream first goes `Pending` and resets on each received chunk.

## Request Forwarding

Protocol-agnostic, full passthrough:
- All paths and methods forwarded (`fallback` handler)
- Request body: `axum::Body → into_data_stream() → reqwest::Body::wrap_stream()`
- Response body: `reqwest::bytes_stream() → PermitBody → axum::Body::new()`
- Auth headers (`x-api-key`, `Authorization`) forwarded as-is; proxy stores no keys

Header handling:
- Strip hop-by-hop headers (Connection, Keep-Alive, TE, Trailers, Transfer-Encoding, Upgrade, Proxy-*)
- Request: also strip Host + Content-Length (reqwest sets its own)
- Response: forward Content-Length (so client knows body size); strip Transfer-Encoding

## Error Responses

| Condition | Status | Body |
|---|---|---|
| Queue timeout (max_wait exceeded) | 503 + `Retry-After` | "throttle: queue timeout" |
| Upstream connection failure | 502 | "throttle: upstream error" |
| Idle timeout (mid-stream) | stream ends | (logged; client sees truncated SSE) |

Never returns 429 — that's the error we exist to prevent.

## Graceful Shutdown

SIGTERM/SIGINT → stop accepting new connections → wait for in-flight bodies to
finish (axum `with_graceful_shutdown`). Queued waiters are cancelled (their
`acquire_owned` futures are dropped). Permits are RAII-released as connections drain.

## Config

```toml
[upstream]
base_url = "https://api.code.umans.ai"

[server]
listen = "127.0.0.1:8080"

[throttle]
max_in_flight = 4      # Code Max=4, Code Pro=3
max_wait = "5m"        # queue timeout
idle_timeout = "2m"    # no-bytes → abort (safety net)
```

## Dependencies

| Crate | Purpose |
|---|---|
| axum 0.8 | HTTP server, routing, body types |
| reqwest 0.12 | HTTP client (stream, rustls-tls, http2) |
| tokio | async runtime, Semaphore, timeout, signals |
| serde + toml | config parsing |
| http-body / http-body-util | Body trait, Frame |
| humantime_serde | "5m" → Duration in config |
| tracing | structured logging |

## Module Structure

```
src/
├── main.rs     # entry: config load, tracing, serve, graceful shutdown
├── lib.rs      # re-exports for integration tests
├── config.rs   # Config struct + TOML deserialization
└── proxy.rs    # AppState, proxy_handler, PermitBody, header filtering
```

## Testing Strategy

Mock upstream (axum server with configurable delay + concurrency tracking) +
real proxy instance. Test via real HTTP from a reqwest client.

Tests:
1. Concurrency: N+1 concurrent requests, verify only N in-flight at upstream
2. Queue timeout: fill slots, verify 503 after max_wait
3. Permit release on completion: request finishes → slot available immediately
4. Permit release on client disconnect: client drops → slot freed
5. Header passthrough: x-api-key forwarded, hop-by-hop stripped
6. Streaming: SSE chunks forwarded in order
7. Path passthrough: query strings preserved
