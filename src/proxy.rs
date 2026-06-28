//! Throttling reverse proxy: forward all requests to upstream with a
//! concurrency semaphore + queue, streaming responses transparently.
//!
//! The permit is held for the entire duration the upstream request is in-flight
//! and is released via RAII when the response body stream ends — on completion,
//! client disconnect, upstream error, or idle timeout.

use crate::config::{Config, UsageSampling};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_core::Stream;
use http_body::Frame;
use reqwest::Client;
use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Sleep;
use tracing::{debug, error, info, warn};

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
    release_grace: Duration,
    usage_sampling: UsageSampling,
    next_request_id: AtomicU64,
    active_in_flight: AtomicUsize,
    grace_pending: AtomicUsize,
    queued: AtomicUsize,
}

#[derive(Debug, Deserialize)]
struct UsageResponse {
    limits: Option<UsageLimits>,
    usage: Option<UsageCounters>,
}

#[derive(Debug, Deserialize)]
struct UsageLimits {
    concurrency: Option<ConcurrencyLimit>,
}

#[derive(Debug, Deserialize)]
struct ConcurrencyLimit {
    limit: Option<u64>,
    hard_cap: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct UsageCounters {
    concurrent_sessions: Option<u64>,
    priority: Option<UsagePriority>,
}

#[derive(Debug, Deserialize)]
struct UsagePriority {
    low: Option<bool>,
}

impl AppState {
    pub fn new(config: &Config) -> Self {
        // Parse the base URL once at startup; per-request we only join the path.
        let upstream_base = reqwest::Url::parse(config.upstream.base_url.trim_end_matches('/'))
            .expect("invalid upstream base_url");

        // reqwest with rustls + ALPN: negotiates h2/h1 automatically.
        // Pool tuned for the proxy's concurrency model: few concurrent
        // upstreams (<= max_in_flight), long-lived streaming connections.
        // connect_timeout guards against a hung TCP connect permanently
        // occupying a permit: without it, one dead connect = one lost slot.
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
                release_grace: config.throttle.release_grace,
                usage_sampling: config.observability.usage_sampling,
                next_request_id: AtomicU64::new(1),
                active_in_flight: AtomicUsize::new(0),
                grace_pending: AtomicUsize::new(0),
                queued: AtomicUsize::new(0),
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

#[derive(Clone, Copy)]
struct TrafficSnapshot {
    available_permits: usize,
    held_permits: usize,
    active_in_flight: usize,
    grace_pending: usize,
    queued: usize,
}

impl Inner {
    fn snapshot(&self) -> TrafficSnapshot {
        let available_permits = self.sem.available_permits();
        TrafficSnapshot {
            available_permits,
            held_permits: self.max_in_flight.saturating_sub(available_permits),
            active_in_flight: self.active_in_flight.load(Ordering::SeqCst),
            grace_pending: self.grace_pending.load(Ordering::SeqCst),
            queued: self.queued.load(Ordering::SeqCst),
        }
    }
}

#[derive(Clone)]
struct RequestTrace {
    inner: Arc<Inner>,
    request_id: u64,
    path: Arc<str>,
    started_at: Instant,
}

impl RequestTrace {
    fn new(inner: Arc<Inner>, path: String) -> Self {
        let request_id = inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        Self {
            inner,
            request_id,
            path: Arc::from(path),
            started_at: Instant::now(),
        }
    }

    fn snapshot(&self) -> TrafficSnapshot {
        self.inner.snapshot()
    }
}

struct AcquiredPermit {
    permit: OwnedSemaphorePermit,
    acquired_at: Instant,
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
    let method_for_log = req.method().clone();
    let trace = RequestTrace::new(state.inner.clone(), req.uri().path().to_string());
    log_request_received(&trace, &method_for_log);

    // 1. Acquire a permit, queueing up to max_wait.
    let acquired = match acquire_permit(&state, &trace).await {
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
            release_acquired_permit(
                &trace,
                acquired.permit,
                acquired.acquired_at,
                "invalid_upstream_path",
            );
            return upstream_error_response();
        }
    };

    // 3. Split the request, strip hop-by-hop headers in-place, stream body.
    let (mut parts, body) = req.into_parts();
    let method = parts.method;
    // Remove hop-by-hop + host + content-length in-place from the owned map.
    // This avoids cloning every header value into a new HeaderMap.
    strip_request_headers(&mut parts.headers);
    let usage_auth_headers = UsageAuthHeaders::from_request(&parts.headers);
    let body_stream = body.into_data_stream();
    let req_body = reqwest::Body::wrap_stream(body_stream);

    // 4. Send to upstream.
    let upstream_req = state
        .inner
        .client
        .request(method, upstream_url.as_str())
        .headers(parts.headers)
        .body(req_body);

    let upstream_started_at = Instant::now();
    let mut upstream_resp = match upstream_req.send().await {
        Ok(r) => {
            log_upstream_headers(&trace, r.status(), r.headers(), upstream_started_at);
            maybe_sample_usage_on_429(&trace, usage_auth_headers, r.status());
            r
        }
        Err(e) => {
            error!(error = %e, url = %upstream_url, "upstream request failed");
            warn!(
                event = "upstream_send_error",
                request_id = trace.request_id,
                path = %trace.path,
                upstream_send_ms = upstream_started_at.elapsed().as_millis() as u64,
                error = %e,
            );
            release_acquired_permit(
                &trace,
                acquired.permit,
                acquired.acquired_at,
                "upstream_send_error",
            );
            return upstream_error_response();
        }
    };

    // 5. Build the streaming response. Steal owned headers via mem::take
    //    (zero-copy: no value cloning), strip hop-by-hop, then move the body
    //    stream into PermitBody which owns the permit until the stream ends.
    let status = upstream_resp.status();
    let mut resp_headers = std::mem::take(upstream_resp.headers_mut());
    strip_response_headers(&mut resp_headers);

    if is_empty_response_body(status, &resp_headers) {
        release_acquired_permit(&trace, acquired.permit, acquired.acquired_at, "complete");
        let mut resp = Response::new(Body::empty());
        *resp.status_mut() = status;
        *resp.headers_mut() = resp_headers;
        return resp;
    }

    let stream = upstream_resp.bytes_stream();
    let body = PermitBody::new(
        stream,
        acquired.permit,
        trace,
        acquired.acquired_at,
        state.inner.idle_timeout,
    );

    let mut resp = Response::new(Body::new(body));
    *resp.status_mut() = status;
    *resp.headers_mut() = resp_headers;
    resp
}

/// Acquire a permit with queue timeout. Returns the permit guard (RAII) or
/// a pre-built error response on timeout/failure.
#[allow(clippy::result_large_err)]
async fn acquire_permit(
    state: &AppState,
    trace: &RequestTrace,
) -> Result<AcquiredPermit, Response> {
    state.inner.queued.fetch_add(1, Ordering::SeqCst);
    match tokio::time::timeout(
        state.inner.max_wait,
        state.inner.sem.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => {
            let queued = state.inner.queued.fetch_sub(1, Ordering::SeqCst) - 1;
            let active_in_flight = state.inner.active_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            let acquired_at = Instant::now();
            let available_permits = state.inner.sem.available_permits();
            log_permit_acquired(
                trace,
                TrafficSnapshot {
                    available_permits,
                    held_permits: state.inner.max_in_flight.saturating_sub(available_permits),
                    active_in_flight,
                    grace_pending: state.inner.grace_pending.load(Ordering::SeqCst),
                    queued,
                },
                acquired_at.duration_since(trace.started_at),
            );
            Ok(AcquiredPermit {
                permit,
                acquired_at,
            })
        }
        Ok(Err(e)) => {
            let queued = state.inner.queued.fetch_sub(1, Ordering::SeqCst) - 1;
            let available_permits = state.inner.sem.available_permits();
            error!(error = %e, "semaphore closed");
            warn!(
                event = "semaphore_closed",
                request_id = trace.request_id,
                path = %trace.path,
                available_permits,
                held_permits = state.inner.max_in_flight.saturating_sub(available_permits),
                active_in_flight = state.inner.active_in_flight.load(Ordering::SeqCst),
                grace_pending = state.inner.grace_pending.load(Ordering::SeqCst),
                queued,
            );
            Err(queue_timeout_response(state.inner.max_wait))
        }
        Err(_) => {
            let queued = state.inner.queued.fetch_sub(1, Ordering::SeqCst) - 1;
            let available_permits = state.inner.sem.available_permits();
            warn!("queue timeout: request waited longer than max_wait");
            log_queue_timeout(
                trace,
                TrafficSnapshot {
                    available_permits,
                    held_permits: state.inner.max_in_flight.saturating_sub(available_permits),
                    active_in_flight: state.inner.active_in_flight.load(Ordering::SeqCst),
                    grace_pending: state.inner.grace_pending.load(Ordering::SeqCst),
                    queued,
                },
                trace.started_at.elapsed(),
            );
            Err(queue_timeout_response(state.inner.max_wait))
        }
    }
}

fn is_messages_path(path: &str) -> bool {
    path == "/v1/messages"
}

fn log_request_received(trace: &RequestTrace, method: &axum::http::Method) {
    let snapshot = trace.snapshot();
    if is_messages_path(&trace.path) {
        info!(
            event = "request_received",
            request_id = trace.request_id,
            method = %method,
            path = %trace.path,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
        );
    } else {
        debug!(
            event = "request_received",
            request_id = trace.request_id,
            method = %method,
            path = %trace.path,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
        );
    }
}

fn log_permit_acquired(trace: &RequestTrace, snapshot: TrafficSnapshot, queue_wait: Duration) {
    if is_messages_path(&trace.path) {
        info!(
            event = "permit_acquired",
            request_id = trace.request_id,
            path = %trace.path,
            queue_wait_ms = queue_wait.as_millis() as u64,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
        );
    } else {
        debug!(
            event = "permit_acquired",
            request_id = trace.request_id,
            path = %trace.path,
            queue_wait_ms = queue_wait.as_millis() as u64,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
        );
    }
}

fn log_queue_timeout(trace: &RequestTrace, snapshot: TrafficSnapshot, wait: Duration) {
    warn!(
        event = "queue_timeout",
        request_id = trace.request_id,
        path = %trace.path,
        wait_ms = wait.as_millis() as u64,
        available_permits = snapshot.available_permits,
        held_permits = snapshot.held_permits,
        active_in_flight = snapshot.active_in_flight,
        grace_pending = snapshot.grace_pending,
        queued = snapshot.queued,
    );
}

fn log_upstream_headers(
    trace: &RequestTrace,
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    upstream_started_at: Instant,
) {
    let snapshot = trace.snapshot();
    let retry_after = header_value(headers, reqwest::header::RETRY_AFTER);
    let upstream_request_id = header_value(headers, "x-request-id")
        .or_else(|| header_value(headers, "request-id"))
        .unwrap_or("");
    let retry_after = retry_after.unwrap_or("");
    let upstream_header_ms = upstream_started_at.elapsed().as_millis() as u64;
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        warn!(
            event = "upstream_headers",
            request_id = trace.request_id,
            path = %trace.path,
            status = status.as_u16(),
            upstream_header_ms,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
            retry_after,
            upstream_request_id,
        );
    } else if is_messages_path(&trace.path) {
        info!(
            event = "upstream_headers",
            request_id = trace.request_id,
            path = %trace.path,
            status = status.as_u16(),
            upstream_header_ms,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
            retry_after,
            upstream_request_id,
        );
    } else {
        debug!(
            event = "upstream_headers",
            request_id = trace.request_id,
            path = %trace.path,
            status = status.as_u16(),
            upstream_header_ms,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
            retry_after,
            upstream_request_id,
        );
    }
}

fn maybe_sample_usage_on_429(
    trace: &RequestTrace,
    auth_headers: UsageAuthHeaders,
    upstream_status: reqwest::StatusCode,
) {
    if upstream_status != reqwest::StatusCode::TOO_MANY_REQUESTS {
        return;
    }
    if trace.inner.usage_sampling != UsageSampling::On429 {
        return;
    }
    if auth_headers.is_empty() {
        let snapshot = trace.snapshot();
        warn!(
            event = "remote_usage_sample_skipped",
            trigger = "upstream_429",
            request_id = trace.request_id,
            path = %trace.path,
            reason = "missing_auth_header",
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
        );
        return;
    }

    let trace = trace.clone();
    tokio::spawn(async move {
        sample_usage_after_429(trace, auth_headers).await;
    });
}

struct UsageAuthHeaders {
    authorization: Option<HeaderValue>,
    x_api_key: Option<HeaderValue>,
}

impl UsageAuthHeaders {
    fn from_request(headers: &HeaderMap) -> Self {
        Self {
            authorization: headers.get(axum::http::header::AUTHORIZATION).cloned(),
            x_api_key: headers.get("x-api-key").cloned(),
        }
    }

    fn is_empty(&self) -> bool {
        self.authorization.is_none() && self.x_api_key.is_none()
    }
}

async fn sample_usage_after_429(trace: RequestTrace, auth_headers: UsageAuthHeaders) {
    let started_at = Instant::now();
    let usage_url = match trace.inner.upstream_base.join("/v1/usage") {
        Ok(url) => url,
        Err(e) => {
            log_remote_usage_error(&trace, started_at, "invalid_usage_url", &e);
            return;
        }
    };

    let mut request = trace.inner.client.get(usage_url);
    if let Some(value) = auth_headers.authorization {
        request = request.header(reqwest::header::AUTHORIZATION, value);
    }
    if let Some(value) = auth_headers.x_api_key {
        request = request.header("x-api-key", value);
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(e) => {
            log_remote_usage_error(&trace, started_at, "send_failed", &e);
            return;
        }
    };
    let status = response.status();
    if !status.is_success() {
        let snapshot = trace.snapshot();
        warn!(
            event = "remote_usage_sample_failed",
            trigger = "upstream_429",
            request_id = trace.request_id,
            path = %trace.path,
            status = status.as_u16(),
            sample_ms = started_at.elapsed().as_millis() as u64,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight = snapshot.active_in_flight,
            grace_pending = snapshot.grace_pending,
            queued = snapshot.queued,
        );
        return;
    }

    match response.json::<UsageResponse>().await {
        Ok(usage) => log_remote_usage_sample(&trace, started_at, usage),
        Err(e) => log_remote_usage_error(&trace, started_at, "decode_failed", &e),
    }
}

fn log_remote_usage_sample(trace: &RequestTrace, started_at: Instant, usage: UsageResponse) {
    let snapshot = trace.snapshot();
    let concurrency = usage
        .limits
        .as_ref()
        .and_then(|limits| limits.concurrency.as_ref());
    let counters = usage.usage.as_ref();
    let priority = counters.and_then(|usage| usage.priority.as_ref());
    info!(
        event = "remote_usage_sample",
        trigger = "upstream_429",
        request_id = trace.request_id,
        path = %trace.path,
        sample_ms = started_at.elapsed().as_millis() as u64,
        available_permits = snapshot.available_permits,
        held_permits = snapshot.held_permits,
        active_in_flight = snapshot.active_in_flight,
        grace_pending = snapshot.grace_pending,
        queued = snapshot.queued,
        remote_concurrent_sessions = counters.and_then(|usage| usage.concurrent_sessions),
        remote_concurrency_limit = concurrency.and_then(|limit| limit.limit),
        remote_concurrency_hard_cap = concurrency.and_then(|limit| limit.hard_cap),
        remote_priority_low = priority.and_then(|priority| priority.low),
    );
}

fn log_remote_usage_error(
    trace: &RequestTrace,
    started_at: Instant,
    reason: &'static str,
    error: &dyn std::fmt::Display,
) {
    let snapshot = trace.snapshot();
    warn!(
        event = "remote_usage_sample_failed",
        trigger = "upstream_429",
        request_id = trace.request_id,
        path = %trace.path,
        reason,
        sample_ms = started_at.elapsed().as_millis() as u64,
        available_permits = snapshot.available_permits,
        held_permits = snapshot.held_permits,
        active_in_flight = snapshot.active_in_flight,
        grace_pending = snapshot.grace_pending,
        queued = snapshot.queued,
        error = %error,
    );
}

fn header_value<N>(headers: &reqwest::header::HeaderMap, name: N) -> Option<&str>
where
    N: reqwest::header::AsHeaderName,
{
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn is_empty_response_body(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> bool {
    status == reqwest::StatusCode::NO_CONTENT
        || status == reqwest::StatusCode::NOT_MODIFIED
        || headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            == Some("0")
}

fn release_acquired_permit(
    trace: &RequestTrace,
    permit: OwnedSemaphorePermit,
    acquired_at: Instant,
    reason: &'static str,
) {
    let trace = trace.clone();
    let release_grace = trace.inner.release_grace;

    let active_in_flight_after = trace
        .inner
        .active_in_flight
        .fetch_sub(1, Ordering::SeqCst)
        .saturating_sub(1);

    if release_grace.is_zero() {
        drop(permit);
        log_permit_released(&trace, acquired_at, reason, release_grace);
        return;
    }

    let grace_pending_after = trace.inner.grace_pending.fetch_add(1, Ordering::SeqCst) + 1;
    log_permit_release_scheduled(
        &trace,
        acquired_at,
        reason,
        release_grace,
        active_in_flight_after,
        grace_pending_after,
    );

    tokio::spawn(async move {
        tokio::time::sleep(release_grace).await;
        drop(permit);
        trace.inner.grace_pending.fetch_sub(1, Ordering::SeqCst);
        log_permit_released(&trace, acquired_at, reason, release_grace);
    });
}

fn log_permit_release_scheduled(
    trace: &RequestTrace,
    acquired_at: Instant,
    reason: &'static str,
    release_grace: Duration,
    active_in_flight_after: usize,
    grace_pending_after: usize,
) {
    let snapshot = trace.snapshot();
    if is_messages_path(&trace.path) {
        info!(
            event = "permit_release_scheduled",
            request_id = trace.request_id,
            path = %trace.path,
            reason,
            held_ms = acquired_at.elapsed().as_millis() as u64,
            release_grace_ms = release_grace.as_millis() as u64,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight_after,
            grace_pending_after,
            queued = snapshot.queued,
        );
    } else {
        debug!(
            event = "permit_release_scheduled",
            request_id = trace.request_id,
            path = %trace.path,
            reason,
            held_ms = acquired_at.elapsed().as_millis() as u64,
            release_grace_ms = release_grace.as_millis() as u64,
            available_permits = snapshot.available_permits,
            held_permits = snapshot.held_permits,
            active_in_flight_after,
            grace_pending_after,
            queued = snapshot.queued,
        );
    }
}

fn log_permit_released(
    trace: &RequestTrace,
    acquired_at: Instant,
    reason: &'static str,
    release_grace: Duration,
) {
    let snapshot = trace.snapshot();
    if is_messages_path(&trace.path) {
        info!(
            event = "permit_released",
            request_id = trace.request_id,
            path = %trace.path,
            reason,
            held_ms = acquired_at.elapsed().as_millis() as u64,
            release_grace_ms = release_grace.as_millis() as u64,
            available_permits_after = snapshot.available_permits,
            held_permits_after = snapshot.held_permits,
            active_in_flight_after = snapshot.active_in_flight,
            grace_pending_after = snapshot.grace_pending,
            queued_after = snapshot.queued,
        );
    } else {
        debug!(
            event = "permit_released",
            request_id = trace.request_id,
            path = %trace.path,
            reason,
            held_ms = acquired_at.elapsed().as_millis() as u64,
            release_grace_ms = release_grace.as_millis() as u64,
            available_permits_after = snapshot.available_permits,
            held_permits_after = snapshot.held_permits,
            active_in_flight_after = snapshot.active_in_flight,
            grace_pending_after = snapshot.grace_pending,
            queued_after = snapshot.queued,
        );
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
    trace: RequestTrace,
    acquired_at: Instant,
    idle_timeout: Duration,
    /// The idle timer, created lazily on first Pending and reused thereafter
    /// via `reset()` — no per-chunk allocation.
    idle_timer: Option<Pin<Box<Sleep>>>,
    idle_timer_armed: bool,
    release_reason: &'static str,
}

impl<S> PermitBody<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    fn new(
        stream: S,
        permit: OwnedSemaphorePermit,
        trace: RequestTrace,
        acquired_at: Instant,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            permit: Some(permit),
            trace,
            acquired_at,
            idle_timeout,
            idle_timer: None,
            idle_timer_armed: false,
            release_reason: "client_drop",
        }
    }

    fn release_permit(&mut self, reason: &'static str) {
        self.release_reason = reason;
        if let Some(permit) = self.permit.take() {
            release_acquired_permit(&self.trace, permit, self.acquired_at, reason);
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
                // Data arrived: the next idle window starts only when the
                // upstream stream goes Pending again.
                this.idle_timer_armed = false;
                Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            Poll::Ready(Some(Err(e))) => {
                warn!(
                    event = "upstream_stream_error",
                    request_id = this.trace.request_id,
                    path = %this.trace.path,
                    error = %e,
                );
                // Stream error: the upstream body is terminal, so release the
                // permit before propagating the error to the client.
                this.release_permit("stream_error");
                Poll::Ready(Some(Err(Box::new(e))))
            }
            Poll::Ready(None) => {
                // Stream ended normally — release permit (via drop).
                this.release_permit("complete");
                Poll::Ready(None)
            }
            Poll::Pending => {
                // Stream not ready: arm the idle timer once for this no-data
                // window and race it against the upstream stream.
                let timer = this
                    .idle_timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(this.idle_timeout)));
                if !this.idle_timer_armed {
                    let deadline = tokio::time::Instant::now() + this.idle_timeout;
                    timer.as_mut().reset(deadline);
                    this.idle_timer_armed = true;
                }
                match timer.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        let snapshot = this.trace.snapshot();
                        warn!(
                            event = "stream_idle_timeout",
                            request_id = this.trace.request_id,
                            path = %this.trace.path,
                            idle_timeout_ms = this.idle_timeout.as_millis() as u64,
                            available_permits = snapshot.available_permits,
                            held_permits = snapshot.held_permits,
                            active_in_flight = snapshot.active_in_flight,
                            grace_pending = snapshot.grace_pending,
                            queued = snapshot.queued,
                        );
                        // Abort: end the body. Permit released on drop.
                        this.release_permit("idle_timeout");
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
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
        if let Some(permit) = self.permit.take() {
            let reason = self.release_reason;
            release_acquired_permit(&self.trace, permit, self.acquired_at, reason);
        }
        debug!("PermitBody dropped, permit released");
    }
}
