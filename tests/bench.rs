//! Microbenchmark: measure pure proxy overhead vs direct upstream.
//!
//! Run: cargo test --release --test bench -- --nocapture --ignored
//!
//! Upstream returns immediately (empty 200). We fire N sequential requests
//! direct and through proxy, measuring only the proxy's added latency.
//! Uses a single reused reqwest connection per target to eliminate TCP
//! handshake noise.

mod common;

use std::time::{Duration, Instant};

use axum::{routing::post, Router};
use tokio::net::TcpListener;

async fn start_instant_upstream() -> String {
    let app = Router::new().route(
        "/{*anything}",
        post(|| async { (axum::http::StatusCode::OK, "") }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
#[ignore]
async fn bench_proxy_overhead() {
    let upstream_url = start_instant_upstream().await;
    let proxy = common::spawn_proxy(
        &upstream_url,
        8,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let n = 2000usize;

    // Warmup (fills connection pools)
    for _ in 0..50 {
        let _ = client.post(&upstream_url).body("x").send().await.unwrap();
        let _ = client.post(&proxy.url).body("x").send().await.unwrap();
    }

    // Direct
    // Direct — 64-byte POST body, same as standalone bench
    let body_bytes = "x".repeat(64);
    let start = Instant::now();
    for _ in 0..n {
        let _ = client
            .post(&upstream_url)
            .body(body_bytes.clone())
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
    }
    let direct = start.elapsed();
    let direct_us = direct.as_micros() as f64 / n as f64;

    // Through proxy
    let start = Instant::now();
    for _ in 0..n {
        let _ = client
            .post(&proxy.url)
            .body(body_bytes.clone())
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
    }
    let proxied = start.elapsed();
    let proxy_us = proxied.as_micros() as f64 / n as f64;

    let overhead_us = proxy_us - direct_us;
    println!("\n=== {n} sequential POST requests, 64-byte body (same-process runtime) ===");
    println!("direct:   {direct_us:.1} µs/req  ({direct:.1?} total)");
    println!("proxy:    {proxy_us:.1} µs/req  ({proxied:.1?} total)");
    println!(
        "overhead: {overhead_us:.1} µs/req  ({:.2}× direct)",
        proxy_us / direct_us
    );
}
