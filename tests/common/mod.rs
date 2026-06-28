//! Test helpers: a mock upstream (axum) + the real proxy instance.
//!
//! The mock records concurrency and lets tests release held slots on demand.
//!
//! Shared across multiple test binaries — not every symbol is used by every
//! binary, so we allow dead code at the module level.
#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::routing::{any, get, post};
use axum::Router;
use bytes::Bytes;
use futures_util::StreamExt;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use umans_throttle::{build_app, AppState, Config};

/// A running proxy instance. Drop is fine — the test process exits.
pub struct ProxyHandle {
    pub url: String,
}

/// A mock upstream that records concurrency and can hold/release slots.
pub struct MockUpstream {
    pub url: String,
    state: Arc<MockState>,
}

struct MockState {
    current: AtomicUsize,
    peak: AtomicUsize,
    total: AtomicUsize,
    last_headers: Mutex<Vec<(String, String)>>,
    /// Senders for requests currently held in Hold mode. release_n drains these.
    held: Mutex<Vec<oneshot::Sender<()>>>,
}

impl MockState {
    fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            last_headers: Mutex::new(Vec::new()),
            held: Mutex::new(Vec::new()),
        }
    }

    fn enter(&self) {
        let c = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        let mut peak = self.peak.load(Ordering::SeqCst);
        while c > peak {
            match self
                .peak
                .compare_exchange(peak, c, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
        self.total.fetch_add(1, Ordering::SeqCst);
    }

    fn exit(&self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MockUpstream {
    /// API-shaped mode: /v1/messages returns 429 and /v1/usage returns a
    /// minimal account usage payload. Used to verify 429-triggered diagnostics.
    pub async fn start_429_with_usage() -> Self {
        let state = Arc::new(MockState::new());
        let app = Router::new()
            .route(
                "/v1/messages",
                post({
                    let state = state.clone();
                    move |req: Request| {
                        let state = state.clone();
                        async move {
                            record_headers(&state, &req);
                            state.enter();
                            state.exit();
                            (
                                StatusCode::TOO_MANY_REQUESTS,
                                [("retry-after", "1"), ("x-request-id", "upstream-test-id")],
                                "rate limited",
                            )
                        }
                    }
                }),
            )
            .route(
                "/v1/usage",
                get({
                    let state = state.clone();
                    move |req: Request| {
                        let state = state.clone();
                        async move {
                            record_headers(&state, &req);
                            (
                                StatusCode::OK,
                                axum::Json(serde_json::json!({
                                    "limits": {
                                        "concurrency": {
                                            "limit": 4,
                                            "hard_cap": 8
                                        }
                                    },
                                    "usage": {
                                        "concurrent_sessions": 7,
                                        "priority": {
                                            "low": true
                                        }
                                    }
                                })),
                            )
                        }
                    }
                }),
            );
        let url = serve(app).await;
        Self { url, state }
    }

    /// Hold mode: each request blocks until `release_n` releases it, then
    /// returns 200 "ok".
    pub async fn start_hold() -> Self {
        let state = Arc::new(MockState::new());
        let app = Router::new().fallback(any({
            let state = state.clone();
            move |req: Request| {
                let state = state.clone();
                async move {
                    record_headers(&state, &req);
                    state.enter();
                    let (tx, rx) = oneshot::channel();
                    state.held.lock().push(tx);
                    let _ = rx.await;
                    state.exit();
                    (StatusCode::OK, "ok")
                }
            }
        }));
        let url = serve(app).await;
        Self { url, state }
    }

    /// Echo mode: returns 200 with body "{path}|{x-api-key}".
    pub async fn start_echo() -> Self {
        let state = Arc::new(MockState::new());
        let app = Router::new().fallback(any({
            let state = state.clone();
            move |req: Request| {
                let state = state.clone();
                async move {
                    record_headers(&state, &req);
                    state.enter();
                    let path = req
                        .uri()
                        .path_and_query()
                        .map(|p| p.to_string())
                        .unwrap_or_default();
                    let api_key = req
                        .headers()
                        .get("x-api-key")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    state.exit();
                    (StatusCode::OK, format!("{path}|{api_key}"))
                }
            }
        }));
        let url = serve(app).await;
        Self { url, state }
    }

    /// Streaming mode: returns 200 with a body streaming the given chunks,
    /// each separated by `delay`.
    pub async fn start_streaming(chunks: Vec<&'static str>, delay: Duration) -> Self {
        let state = Arc::new(MockState::new());
        let app = Router::new().fallback(any({
            let state = state.clone();
            move |req: Request| {
                let state = state.clone();
                async move {
                    record_headers(&state, &req);
                    state.enter();
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
                    let chunks = chunks.clone();
                    tokio::spawn(async move {
                        for chunk in chunks {
                            let _ = tx.send(Bytes::from(chunk));
                            tokio::time::sleep(delay).await;
                        }
                        // tx dropped → stream ends
                        state.exit();
                    });
                    let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
                        .map(Ok::<bytes::Bytes, std::convert::Infallible>);
                    (StatusCode::OK, Body::from_stream(stream))
                }
            }
        }));
        let url = serve(app).await;
        Self { url, state }
    }

    /// Streaming-hold mode: emits one chunk after `initial_delay`, then blocks
    /// forever (stream never ends). Used to test client-disconnect permit release
    /// with a genuinely in-flight stream.
    pub async fn start_streaming_hold(initial_delay: Duration) -> Self {
        let state = Arc::new(MockState::new());
        let app = Router::new().fallback(any({
            let state = state.clone();
            move |req: Request| {
                let state = state.clone();
                async move {
                    record_headers(&state, &req);
                    state.enter();
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
                    tokio::spawn(async move {
                        // Emit one chunk so the stream is active, then
                        // never send again — stream stays open forever.
                        tokio::time::sleep(initial_delay).await;
                        let _ = tx.send(Bytes::from("data: streaming\n\n"));
                        // hold tx open: stream never ends
                    });
                    let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
                        .map(Ok::<bytes::Bytes, std::convert::Infallible>);
                    (StatusCode::OK, Body::from_stream(stream))
                }
            }
        }));
        let url = serve(app).await;
        Self { url, state }
    }

    /// Error mode: returns the given HTTP status with an empty body.
    pub async fn start_error(status: axum::http::StatusCode) -> Self {
        let state = Arc::new(MockState::new());
        let app = Router::new().fallback(any({
            let state = state.clone();
            move |req: Request| {
                let state = state.clone();
                async move {
                    record_headers(&state, &req);
                    state.enter();
                    state.exit();
                    (status, "")
                }
            }
        }));
        let url = serve(app).await;
        Self { url, state }
    }

    pub fn peak_concurrency(&self) -> usize {
        self.state.peak.load(Ordering::SeqCst)
    }

    pub fn total_requests(&self) -> usize {
        self.state.total.load(Ordering::SeqCst)
    }

    /// Release `n` held requests.
    pub fn release_n(&self, n: usize) {
        let senders: Vec<_> = {
            let mut held = self.state.held.lock();
            let take = n.min(held.len());
            held.drain(..take).collect()
        };
        for tx in senders {
            let _ = tx.send(());
        }
    }

    pub fn last_request_headers(&self) -> Vec<(String, String)> {
        self.state.last_headers.lock().clone()
    }
}

fn record_headers(state: &MockState, req: &Request) {
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    *state.last_headers.lock() = headers;
}

/// Bind an axum app to a random localhost port and return its URL.
async fn serve(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

/// Spawn the real proxy pointed at `upstream_url`.
pub async fn spawn_proxy(
    upstream_url: &str,
    max_in_flight: usize,
    max_wait: Duration,
    idle_timeout: Duration,
) -> ProxyHandle {
    spawn_proxy_with_options(
        upstream_url,
        max_in_flight,
        max_wait,
        idle_timeout,
        Duration::from_millis(0),
        "off",
    )
    .await
}

/// Spawn the real proxy pointed at `upstream_url`, with observability/throttle
/// options used by targeted tests.
pub async fn spawn_proxy_with_options(
    upstream_url: &str,
    max_in_flight: usize,
    max_wait: Duration,
    idle_timeout: Duration,
    release_grace: Duration,
    usage_sampling: &str,
) -> ProxyHandle {
    let cfg_str = format!(
        r#"
[upstream]
base_url = "{upstream_url}"

[server]
listen = "127.0.0.1:0"

[throttle]
max_in_flight = {max_in_flight}
max_wait = "{max_wait_str}"
idle_timeout = "{idle_timeout_str}"
release_grace = "{release_grace_str}"

[observability]
usage_sampling = "{usage_sampling}"
"#,
        max_wait_str = humantime::format_duration(max_wait),
        idle_timeout_str = humantime::format_duration(idle_timeout),
        release_grace_str = humantime::format_duration(release_grace),
    );
    let config = Config::parse_str(&cfg_str).unwrap();
    let state = AppState::new(&config);
    let app = build_app(state);
    let url = serve(app).await;
    ProxyHandle { url }
}

/// Spawn the proxy with a manual shutdown signal. Returns the URL and a
/// sender that triggers graceful shutdown when signaled.
pub async fn spawn_proxy_with_shutdown(
    upstream_url: &str,
    max_in_flight: usize,
    max_wait: Duration,
    idle_timeout: Duration,
    shutdown_timeout: Duration,
) -> (ProxyHandle, tokio::sync::oneshot::Sender<()>) {
    let cfg_str = format!(
        r#"
[upstream]
base_url = "{upstream_url}"

[server]
listen = "127.0.0.1:0"
shutdown_timeout = "{shutdown_timeout_str}"

[throttle]
max_in_flight = {max_in_flight}
max_wait = "{max_wait_str}"
idle_timeout = "{idle_timeout_str}"
"#,
        max_wait_str = humantime::format_duration(max_wait),
        idle_timeout_str = humantime::format_duration(idle_timeout),
        shutdown_timeout_str = humantime::format_duration(shutdown_timeout),
    );
    let config = Config::parse_str(&cfg_str).unwrap();
    let state = AppState::new(&config);
    let app = build_app(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let shutdown = async {
            let _ = rx.await;
        };
        let serve = axum::serve(listener, app.into_make_service()).with_graceful_shutdown(shutdown);
        let _ = tokio::time::timeout(shutdown_timeout, serve).await;
    });
    (
        ProxyHandle {
            url: format!("http://{addr}"),
        },
        tx,
    )
}
