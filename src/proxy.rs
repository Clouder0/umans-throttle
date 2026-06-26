//! Throttling reverse proxy: forward all requests to upstream with a
//! concurrency semaphore + queue, streaming responses transparently.
//!
//! The permit is held for the entire duration the upstream request is in-flight
//! and is released via RAII when the response body stream ends — on completion,
//! client disconnect, upstream error, or idle timeout.

use crate::config::Config;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_core::Stream;
use http_body::Frame;
use reqwest::Client;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Sleep;
use tracing::{debug, error, warn};

/// Hop-by-hop headers that must not be forwarded (RFC 7230 §6.1).
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

/// Shared application state, cheaply cloneable (inner is Arc).
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    /// Parsed base URL; reused via `.join(path)` per request — avoids
    /// re-parsing the base on every request.
    upstream_base: reqwest::Url,
    sem: Arc<Semaphore>,
    max_in_flight: usize,
    max_wait: Duration,
    idle_timeout: Duration,
}

impl AppState {
    pub fn new(config: &Config) -> Self {
        // Parse the base URL once at startup; per-request we only join the path.
        let upstream_base = reqwest::Url::parse(config.upstream.base_url.trim_end_matches('/'))
            .expect("invalid upstream base_url");

        // reqwest with rustls + ALPN: negotiates h2/h1 automatically.
        // Pool tuned for the proxy's concurrency model: few concurrent
        // upstreams (≤ max_in_flight), long-lived streaming connections.
        // connect_timeout guards against a hung TCP connect permanently
        // occupying a permit — without it, one dead connect = one lost slot.
        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(config.throttle.max_in_flight)
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client");

        Self {
            inner: Arc::new(Inner {
                client,
                upstream_base,
                sem: Arc::new(Semaphore::new(config.throttle.max_in_flight)),
                max_in_flight: config.throttle.max_in_flight,
                max_wait: config.throttle.max_wait,
                idle_timeout: config.throttle.idle_timeout,
            }),
        }
    }

    /// Number of available permits right now (for diagnostics / testing).
    pub fn available_permits(&self) -> usize {
        self.inner.sem.available_permits()
    }

    pub fn max_in_flight(&self) -> usize {
        self.inner.max_in_flight
    }
}

/// Build the axum router. A single fallback handler forwards everything.
/// Body limit is 10MB — large enough for big context requests, small enough
/// to prevent OOM from a runaway client.
pub fn build_app(state: AppState) -> axum::Router {
    axum::Router::new()
        .fallback(proxy_handler)
        .with_state(state)
        .layer(tower_http::limit::RequestBodyLimitLayer::new(
            10 * 1024 * 1024,
        ))
}

/// The main proxy handler. Acquires a permit (or queues up to max_wait),
/// forwards the request, and returns a streaming response whose body owns
/// the permit until the stream ends.
pub async fn proxy_handler(State(state): State<AppState>, req: Request) -> Response {
    // 1. Acquire a permit, queueing up to max_wait.
    let permit = match acquire_permit(&state).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    debug!("permit acquired");

    // 2. Build the upstream URL via Url::join — single allocation, no format!
    let path = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let upstream_url = match state.inner.upstream_base.join(path) {
        Ok(u) => u,
        Err(e) => {
            error!(error = %e, path = %path, "invalid path for upstream base url");
            return upstream_error_response();
        }
    };

    // 3. Split the request, strip hop-by-hop headers in-place, stream body.
    let (mut parts, body) = req.into_parts();
    let method = parts.method;
    // Remove hop-by-hop + host + content-length in-place from the owned map.
    // This avoids cloning every header value into a new HeaderMap.
    strip_request_headers(&mut parts.headers);
    let body_stream = body.into_data_stream();
    let req_body = reqwest::Body::wrap_stream(body_stream);

    // 4. Send to upstream.
    let upstream_req = state
        .inner
        .client
        .request(method, upstream_url.as_str())
        .headers(parts.headers)
        .body(req_body);

    let mut upstream_resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, url = %upstream_url, "upstream request failed");
            return upstream_error_response();
        }
    };

    // 5. Build the streaming response. Steal owned headers via mem::take
    //    (zero-copy: no value cloning), strip hop-by-hop, then move the body
    //    stream into PermitBody which owns the permit until the stream ends.
    let status = upstream_resp.status();
    let mut resp_headers = std::mem::take(upstream_resp.headers_mut());
    strip_response_headers(&mut resp_headers);

    let stream = upstream_resp.bytes_stream();
    let body = PermitBody::new(stream, permit, state.inner.idle_timeout);

    let mut resp = Response::new(Body::new(body));
    *resp.status_mut() = status;
    *resp.headers_mut() = resp_headers;
    resp
}

/// Acquire a permit with queue timeout. Returns the permit guard (RAII) or
/// a pre-built error response on timeout/failure.
async fn acquire_permit(state: &AppState) -> Result<OwnedSemaphorePermit, Response> {
    match tokio::time::timeout(
        state.inner.max_wait,
        state.inner.sem.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(p)) => Ok(p),
        Ok(Err(e)) => {
            error!(error = %e, "semaphore closed");
            Err(queue_timeout_response(state.inner.max_wait))
        }
        Err(_) => {
            warn!("queue timeout: request waited longer than max_wait");
            Err(queue_timeout_response(state.inner.max_wait))
        }
    }
}

// ---------------------------------------------------------------------------
// Header filtering (in-place — zero value cloning)
// ---------------------------------------------------------------------------

/// Strip hop-by-hop + host + content-length from the request's owned
/// HeaderMap in-place. Uses targeted `remove` calls rather than rebuilding
/// the map, so header values are never cloned.
fn strip_request_headers(headers: &mut HeaderMap) {
    for &name in HOP_BY_HOP {
        while headers.remove(name).is_some() {}
    }
    while headers.remove("host").is_some() {}
    while headers.remove("content-length").is_some() {}
}

/// Strip hop-by-hop + transfer-encoding from the response's owned HeaderMap
/// in-place.
fn strip_response_headers(headers: &mut HeaderMap) {
    for &name in HOP_BY_HOP {
        while headers.remove(name).is_some() {}
    }
    // transfer-encoding is in HOP_BY_HOP already, but be explicit for clarity.
    while headers.remove("transfer-encoding").is_some() {}
}

// ---------------------------------------------------------------------------
// Error responses
// ---------------------------------------------------------------------------

fn queue_timeout_response(max_wait: Duration) -> Response {
    let retry_after = (max_wait.as_secs()).max(1);
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(
            axum::http::header::RETRY_AFTER,
            HeaderValue::from(retry_after),
        )],
        "throttle: queue timeout\n",
    )
        .into_response()
}

fn upstream_error_response() -> Response {
    (StatusCode::BAD_GATEWAY, "throttle: upstream error\n").into_response()
}

// ---------------------------------------------------------------------------
// PermitBody — the response body that owns the permit
// ---------------------------------------------------------------------------

/// A streaming body that owns the semaphore permit. The permit is released when
/// this body is dropped (natural end, client disconnect, idle timeout, or error).
///
/// Idle timeout: if `poll_frame` goes `Pending` (no data ready) we start/reset a
/// `Sleep`. If the sleep fires before data arrives, we abort the stream — returns
/// `Poll::Ready(None)` to end the body. This prevents a hung upstream from
/// permanently occupying a slot.
pub struct PermitBody<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    stream: Pin<Box<S>>,
    permit: Option<OwnedSemaphorePermit>,
    idle_timeout: Duration,
    /// The idle timer, created lazily on first Pending and reused thereafter
    /// via `reset()` — no per-chunk allocation.
    idle_timer: Option<Pin<Box<Sleep>>>,
}

impl<S> PermitBody<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    pub fn new(stream: S, permit: OwnedSemaphorePermit, idle_timeout: Duration) -> Self {
        Self {
            stream: Box::pin(stream),
            permit: Some(permit),
            idle_timeout,
            idle_timer: None,
        }
    }
}

impl<S> http_body::Body for PermitBody<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        // Try the stream first.
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                // Data arrived — disarm (but keep) the idle timer so it can be
                // reused without re-allocating on the next Pending.
                if let Some(timer) = this.idle_timer.as_mut() {
                    let deadline = tokio::time::Instant::now() + this.idle_timeout;
                    timer.as_mut().reset(deadline);
                }
                Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            Poll::Ready(Some(Err(e))) => {
                // Stream error — end the body. Permit released on drop.
                Poll::Ready(Some(Err(Box::new(e))))
            }
            Poll::Ready(None) => {
                // Stream ended normally — release permit (via drop).
                this.permit.take();
                Poll::Ready(None)
            }
            Poll::Pending => {
                // Stream not ready — arm/restart the idle timer and race it
                // against the stream. If the timer fires first, abort.
                let timer = this
                    .idle_timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(this.idle_timeout)));
                let deadline = tokio::time::Instant::now() + this.idle_timeout;
                timer.as_mut().reset(deadline);
                match timer.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        warn!(
                            "idle timeout: no data from upstream within {:?}",
                            this.idle_timeout
                        );
                        // Abort: end the body. Permit released on drop.
                        this.permit.take();
                        Poll::Ready(None)
                    }
                    Poll::Pending => {
                        // Neither stream nor timer ready — yield. The waker
                        // re-invokes poll_frame when either makes progress.
                        Poll::Pending
                    }
                }
            }
        }
    }
}

impl<S> Drop for PermitBody<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    fn drop(&mut self) {
        // RAII: release the permit if not already taken.
        self.permit.take();
        debug!("PermitBody dropped, permit released");
    }
}
