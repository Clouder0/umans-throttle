//! umans-throttle — entry point.
//!
//! Loads config, sets up tracing, and serves the throttling proxy with
//! graceful shutdown on SIGINT/SIGTERM.

use std::future::IntoFuture;
use std::path::PathBuf;

use clap::Parser;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use umans_throttle::{build_app, AppState, Config};

#[derive(Debug, Parser)]
#[command(
    name = "umans-throttle",
    about = "Throttling reverse proxy for the Umans Code API"
)]
struct Cli {
    /// Path to the config file (TOML).
    #[arg(
        short,
        long,
        default_value = "config.toml",
        env = "UMANS_THROTTLE_CONFIG"
    )]
    config: PathBuf,
}

fn main() {
    // tracing: respect RUST_LOG, default to info.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load config from {}: {e}", cli.config.display());
            std::process::exit(2);
        }
    };

    info!(listen = %config.server.listen, "umans-throttle starting");
    info!(
        upstream = %config.upstream.base_url,
        max_in_flight = config.throttle.max_in_flight,
        max_wait = ?config.throttle.max_wait,
        idle_timeout = ?config.throttle.idle_timeout,
        release_grace = ?config.throttle.release_grace,
        usage_sampling = ?config.observability.usage_sampling,
        "throttle config"
    );

    let listen = config.server.listen;
    let state = AppState::new(&config);
    let app = build_app(state);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build runtime")
        .block_on(async move {
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .expect("failed to bind listen address");
            let local = listener.local_addr().expect("local addr");
            let shutdown_timeout = config.server.shutdown_timeout;
            info!(addr = %local, "listening");

            let shutdown_signal = async {
                let ctrl_c = async {
                    tokio::signal::ctrl_c()
                        .await
                        .expect("failed to install ctrl-c handler");
                };

                #[cfg(unix)]
                let term = async {
                    use tokio::signal::unix::{signal, SignalKind};
                    let mut s = signal(SignalKind::terminate())
                        .expect("failed to install SIGTERM handler");
                    s.recv().await;
                };

                #[cfg(not(unix))]
                let term = std::future::pending::<()>();

                tokio::select! {
                    _ = ctrl_c => "SIGINT",
                    _ = term => "SIGTERM",
                }
            };

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let serve = axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .into_future();
            tokio::pin!(serve);

            tokio::select! {
                result = &mut serve => {
                    match result {
                        Ok(()) => info!("server exited"),
                        Err(e) => error!("server error: {e}"),
                    }
                }
                signal = shutdown_signal => {
                    info!(signal, "received shutdown signal, shutting down");
                    let _ = shutdown_tx.send(());

                    // Stop accepting new connections; axum keeps polling existing ones
                    // so in-flight requests can finish. Apply the deadline only after
                    // a shutdown signal, not to normal server uptime.
                    match tokio::time::timeout(shutdown_timeout, &mut serve).await {
                        Ok(Ok(())) => info!("shutdown complete"),
                        Ok(Err(e)) => error!("server error during shutdown: {e}"),
                        Err(_) => {
                            warn!(
                                "shutdown deadline ({:?}) exceeded; forcing exit with in-flight requests",
                                shutdown_timeout
                            );
                        }
                    }
                }
            };
        });
}
