//! Integration tests for the throttling proxy.
//!
//! Strategy: spin up a mock upstream (axum) that records concurrency and
//! can hold/release slots, then run the real proxy pointed at it, then
//! assert behavior via a reqwest client.

mod common;

use std::time::Duration;

use common::{spawn_proxy, MockUpstream};
use http::StatusCode;

#[tokio::test]
async fn concurrency_limit_enforced() {
    // max_in_flight = 2. Fire 4 concurrent requests, each holding the upstream
    // slot until released. Assert the mock never sees more than 2 in-flight.
    let mock = MockUpstream::start_hold().await;

    let proxy = spawn_proxy(
        &mock.url,
        2,                       // max_in_flight
        Duration::from_secs(5),  // max_wait
        Duration::from_secs(30), // idle_timeout
    )
    .await;

    let client = reqwest::Client::new();
    let mut handles = Vec::new();

    for _ in 0..4 {
        let url = proxy.url.clone();
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.post(&url)
                .body("x")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        }));
    }

    // Let them queue up. 2 should be in-flight at upstream; 2 waiting.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let peak1 = mock.peak_concurrency();
    assert!(peak1 <= 2, "concurrency {peak1} exceeded limit 2");

    mock.release_n(2); // release first batch → next 2 proceed
    tokio::time::sleep(Duration::from_millis(200)).await;
    mock.release_n(2); // release second batch

    for h in handles {
        let _ = h.await;
    }

    assert_eq!(mock.total_requests(), 4);
}

#[tokio::test]
async fn queue_timeout_returns_503_with_retry_after() {
    // max_in_flight = 1, max_wait = 100ms. Occupy the single slot, then send a
    // second request — it should queue, time out, and return 503 + Retry-After.
    let mock = MockUpstream::start_hold().await;

    let proxy = spawn_proxy(
        &mock.url,
        1,                          // max_in_flight
        Duration::from_millis(100), // max_wait — very short
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();

    // Occupy the slot (never release during the test).
    let hold = tokio::spawn({
        let url = proxy.url.clone();
        let c = client.clone();
        async move { c.post(&url).body("hold").send().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // This request should queue and time out.
    let resp = client.post(&proxy.url).body("queued").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let retry_after = resp.headers().get("retry-after");
    assert!(retry_after.is_some(), "Retry-After header missing");

    // Cleanup.
    mock.release_n(1);
    let _ = hold.await;
}

#[tokio::test]
async fn permit_released_on_completion() {
    // After a request completes normally, the slot must be immediately
    // available for the next request (no leak).
    let mock = MockUpstream::start_echo().await;

    let proxy = spawn_proxy(
        &mock.url,
        1,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();

    // First request completes fully.
    let r1 = client.post(&proxy.url).body("a").send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let _ = r1.text().await;

    // Second request should succeed immediately (permit was released).
    let r2 = client.post(&proxy.url).body("b").send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::OK);

    assert_eq!(mock.total_requests(), 2);
}

#[tokio::test]
async fn permit_released_on_client_disconnect() {
    // Client starts a streaming request then drops the connection mid-stream.
    // The permit must be released so a subsequent request can proceed.
    //
    // We use a streaming mock that emits one chunk then blocks (never ends),
    // so the upstream stream is genuinely in-flight when the client drops.
    let mock = MockUpstream::start_streaming_hold(Duration::from_millis(50)).await;

    let proxy = spawn_proxy(
        &mock.url,
        1,
        Duration::from_secs(2), // short max_wait so test fails fast if permit leaks
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();

    // Start a streaming request and drop the response handle mid-stream.
    let _resp = client
        .post(&proxy.url)
        .body("will-drop")
        .send()
        .await
        .unwrap();
    // Read one chunk to ensure the stream is active, then drop.
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(_resp);

    // Give the proxy a moment to notice the dropped body and release the permit.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now a new request should proceed without waiting for max_wait (2s).
    // If the permit leaked, this would hang ~2s then 503. We assert immediate 200.
    let start = std::time::Instant::now();
    let r2 = client.post(&proxy.url).body("after").send().await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(r2.status(), StatusCode::OK);
    assert!(
        elapsed < Duration::from_secs(2),
        "second request took {elapsed:?} — permit likely leaked"
    );
}

#[tokio::test]
async fn header_and_path_passthrough() {
    let mock = MockUpstream::start_echo().await;

    let proxy = spawn_proxy(
        &mock.url,
        2,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}{}?foo=bar", proxy.url, "/v1/messages"))
        .header("x-api-key", "sk-test-key")
        .header("x-custom", "keep-me")
        .header("connection", "keep-alive") // hop-by-hop: must be stripped
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "/v1/messages?foo=bar|sk-test-key");

    // Hop-by-hop Connection header should NOT be forwarded to upstream.
    let received = mock.last_request_headers();
    assert!(
        !received
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("connection")),
        "hop-by-hop Connection header was forwarded"
    );
    assert!(
        received
            .iter()
            .any(|(k, v)| { k.eq_ignore_ascii_case("x-custom") && v == "keep-me" }),
        "custom header not forwarded"
    );
}

#[tokio::test]
async fn streaming_sse_passthrough() {
    // Upstream streams 3 SSE chunks with delays; verify they arrive in order.
    let mock = MockUpstream::start_streaming(
        vec!["data: one\n\n", "data: two\n\n", "data: three\n\n"],
        Duration::from_millis(50),
    )
    .await;

    let proxy = spawn_proxy(
        &mock.url,
        1,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let resp = client
        .post(&proxy.url)
        .body("stream me")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    use futures_util::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut received = String::new();
    while let Some(chunk) = stream.next().await {
        received.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    assert_eq!(received, "data: one\n\ndata: two\n\ndata: three\n\n");
}

#[tokio::test]
async fn idle_timeout_aborts_hung_stream_and_releases_permit() {
    // Upstream streams one chunk then hangs forever. With a short idle_timeout,
    // the proxy must abort the stream (idle timeout) and release the permit so
    // a subsequent request proceeds without waiting for max_wait.
    let mock = MockUpstream::start_streaming_hold(Duration::from_millis(20)).await;

    let proxy = spawn_proxy(
        &mock.url,
        1,
        Duration::from_secs(5), // max_wait: long enough that a leak would hang
        Duration::from_millis(300), // idle_timeout: short — abort after 300ms no data
    )
    .await;

    let client = reqwest::Client::new();

    // Start the streaming request. It emits one chunk (~20ms), then hangs.
    let resp = client.post(&proxy.url).body("hung").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    use futures_util::StreamExt;
    let mut stream = resp.bytes_stream();
    // Read the one chunk, then the next poll should hang until idle_timeout fires.
    let first = stream.next().await;
    assert!(first.is_some(), "expected the initial chunk");

    // The stream should end (None) after idle_timeout — not hang forever.
    // Wrap in a timeout so the test fails fast if the safety net is broken.
    let ended = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(
        matches!(ended, Ok(None)),
        "stream did not end after idle_timeout — safety net broken"
    );

    // Permit must now be released: a new request should proceed immediately.
    let start = std::time::Instant::now();
    let r2 = client.post(&proxy.url).body("after").send().await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(r2.status(), StatusCode::OK);
    assert!(
        elapsed < Duration::from_secs(2),
        "second request took {elapsed:?} — permit leaked after idle timeout"
    );
}

#[tokio::test]
async fn upstream_error_status_passthrough() {
    // Upstream returns 503; proxy must forward that status (not synthesize 502).
    // 502 is only for connection failures, not for upstream HTTP error responses.
    let mock = MockUpstream::start_error(StatusCode::BAD_GATEWAY).await;

    let proxy = spawn_proxy(
        &mock.url,
        1,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let resp = client.post(&proxy.url).body("x").send().await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_GATEWAY,
        "upstream HTTP error status should pass through"
    );
}

#[tokio::test]
async fn graceful_shutdown_drains_inflight_then_exits() {
    // Start a request that holds at the upstream. Trigger graceful shutdown.
    // The proxy should let the in-flight request finish (after we release it),
    // then exit within shutdown_timeout. New requests after shutdown signal
    // should fail (connection refused).
    let mock = MockUpstream::start_hold().await;

    let (proxy, shutdown_tx) = common::spawn_proxy_with_shutdown(
        &mock.url,
        1,
        Duration::from_secs(5),
        Duration::from_secs(30),
        Duration::from_secs(5), // shutdown_timeout
    )
    .await;

    let client = reqwest::Client::new();

    // Start an in-flight request (held at upstream).
    let in_flight = {
        let url = proxy.url.clone();
        let c = client.clone();
        tokio::spawn(async move {
            c.post(&url)
                .body("in-flight")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Trigger graceful shutdown.
    let _ = shutdown_tx.send(());

    // Release the held request so it can complete during drain.
    mock.release_n(1);

    // The in-flight request should still complete (drain worked).
    let result = tokio::time::timeout(Duration::from_secs(3), in_flight).await;
    assert!(
        result.is_ok(),
        "in-flight request did not complete during graceful drain"
    );
    assert_eq!(result.unwrap().unwrap(), "ok");

    // After drain, new connections should be refused (server stopped).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let post_shutdown = client.post(&proxy.url).body("after").send().await;
    assert!(
        post_shutdown.is_err(),
        "server should not accept new connections after shutdown"
    );
}

#[tokio::test]
async fn config_rejects_max_in_flight_zero() {
    let cfg_str = r#"
[upstream]
base_url = "http://127.0.0.1:1"

[server]
listen = "127.0.0.1:0"

[throttle]
max_in_flight = 0
"#;
    let result = umans_throttle::Config::parse_str(cfg_str);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("max_in_flight"),
        "error should mention max_in_flight: {err}"
    );
}

#[tokio::test]
async fn body_limit_rejects_oversized_request() {
    // 10MB limit. Send 11MB body — should get rejected (413 or connection reset).
    let mock = MockUpstream::start_echo().await;
    let proxy = spawn_proxy(
        &mock.url,
        1,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let big_body = vec![b'x'; 11 * 1024 * 1024];
    let resp = client.post(&proxy.url).body(big_body).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
