mod batcher;
mod config;
mod event;
mod health;
mod pipeline;
mod queue;
mod reload;
mod sinks;
mod sources;
mod transforms;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use axum::{Router, routing::get};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::health::HealthState;
use crate::reload::PipelineManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // Initialize Prometheus metrics
    // We install the recorder but manage the HTTP server ourselves to add /health
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    let metrics_port = std::env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9100".to_string())
        .parse::<u16>()
        .unwrap_or(9100);

    let shutdown = CancellationToken::new();

    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("Ctrl+C received, shutting down...");
                shutdown.cancel();

                // Close stdin to unblock any tasks stuck on reading it (common hang cause)
                #[cfg(unix)]
                unsafe {
                    libc::close(0);
                }

                // Second Ctrl+C will force exit if graceful shutdown stalls
                if tokio::signal::ctrl_c().await.is_ok() {
                    warn!("Second Ctrl+C received, forcing exit");
                    std::process::exit(130);
                }
            }
        });
    }
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            // Give tasks a grace period; then exit if still alive.
            tokio::time::sleep(Duration::from_secs(10)).await;
            warn!("Forcing exit after grace period");
            std::process::exit(130);
        });
    }

    // Spawn Metrics & Health server
    let health = HealthState::new();
    let health_clone = health.clone();
    let metrics_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(move || std::future::ready(handle.render())))
            .route(
                "/health",
                get(move || {
                    let healthy = health_clone.is_healthy(60);
                    async move { if healthy { "UP" } else { "DOWN" } }
                }),
        );

        let addr = SocketAddr::from(([0, 0, 0, 0], metrics_port));
        info!("Metrics and Health API listening on {}", addr);

        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                let shutdown_signal = metrics_shutdown.clone();
                if let Err(e) = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        shutdown_signal.cancelled().await;
                    })
                    .await
                {
                    warn!("Metrics server error: {}", e);
                }
            }
            Err(e) => {
                warn!("Failed to bind metrics port {}: {}", metrics_port, e);
            }
        }
    });

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("mini_vector.yml"));

    let manager = PipelineManager::new(config_path, health);
    manager.run(shutdown).await
}
