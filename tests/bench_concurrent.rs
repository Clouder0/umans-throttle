//! Concurrent benchmark: measure throughput under contention.
//!
//! Run: cargo test --release --test bench_concurrent -- --nocapture --ignored
//!
//! Tests:
//! 1. Throughput at max concurrency (no contention)
//! 2. Throughput with queueing (2× max_in_flight requests)
//! 3. Peak concurrent upstream requests never exceeds limit

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::routing::any;
use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

struct ConcurrencyTracker {
    current: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    held: Arc<parking_lot::Mutex<Vec<oneshot::Sender<()>>>>,
}

impl ConcurrencyTracker {
    fn new() -> Self {
        Self {
            current: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            held: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    async fn start_upstream(instant: bool) -> (String, ConcurrencyTracker) {
        let tracker = ConcurrencyTracker::new();
        let current = tracker.current.clone();
        let peak = tracker.peak.clone();
        let held = tracker.held.clone();

        let app = Router::new().fallback(any(move |_req: Request| {
            let current = current.clone();
            let peak = peak.clone();
            let held = held.clone();
            async move {
                let c = current.fetch_add(1, Ordering::SeqCst) + 1;
                let mut p = peak.load(Ordering::SeqCst);
                while c > p {
                    match peak.compare_exchange(p, c, Ordering::SeqCst, Ordering::SeqCst) {
                        Ok(_) => break,
                        Err(n) => p = n,
                    }
                }
                if !instant {
                    let (tx, rx) = oneshot::channel();
                    held.lock().push(tx);
                    let _ = rx.await;
                }
                current.fetch_sub(1, Ordering::SeqCst);
                (axum::http::StatusCode::OK, "")
            }
        }));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });
        (format!("http://{addr}"), tracker)
    }

    #[allow(dead_code)]
    fn release_n(&self, n: usize) {
        let senders: Vec<_> = {
            let mut held = self.held.lock();
            let take = n.min(held.len());
            held.drain(..take).collect()
        };
        for tx in senders {
            let _ = tx.send(());
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

#[tokio::test]
#[ignore]
async fn bench_throughput_no_contention() {
    // max_in_flight=8, fire 8 concurrent (no queueing). Measure throughput.
    let (upstream_url, tracker) = ConcurrencyTracker::start_upstream(true).await;
    let proxy = common::spawn_proxy(
        &upstream_url,
        8,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let n = 500usize;

    // Warmup
    for _ in 0..20 {
        let _ = client.post(&proxy.url).body("x").send().await.unwrap();
    }

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..n {
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
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    println!("\n=== {n} concurrent requests, max_in_flight=8 (no contention) ===");
    println!("total:    {elapsed:.1?}");
    println!("rps:      {:.0}", n as f64 / elapsed.as_secs_f64());
    println!(
        "latency:  {:.1} µs/req",
        elapsed.as_micros() as f64 / n as f64
    );
    println!("peak upstream concurrency: {} (limit 8)", tracker.peak());
    assert!(tracker.peak() <= 8, "concurrency limit violated!");
}

#[tokio::test]
#[ignore]
async fn bench_throughput_high_concurrency() {
    // max_in_flight=4, fire 100 concurrent requests against instant upstream.
    // Verify concurrency never exceeds 4, measure throughput under contention.
    let (upstream_url, tracker) = ConcurrencyTracker::start_upstream(true).await;
    let proxy = common::spawn_proxy(
        &upstream_url,
        4,
        Duration::from_secs(30),
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let n = 100usize;

    // Warmup
    for _ in 0..10 {
        let _ = client.post(&proxy.url).body("x").send().await.unwrap();
    }

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..n {
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
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    println!("\n=== {n} concurrent, max_in_flight=4 (contention, instant upstream) ===");
    println!("total:    {elapsed:.1?}");
    println!("rps:      {:.0}", n as f64 / elapsed.as_secs_f64());
    println!(
        "latency:  {:.1} µs/req",
        elapsed.as_micros() as f64 / n as f64
    );
    println!("peak upstream concurrency: {} (limit 4)", tracker.peak());
    assert!(tracker.peak() <= 4, "concurrency limit violated!");
}
