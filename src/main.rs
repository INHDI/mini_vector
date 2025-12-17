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

use axum::{Router, routing::get};
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

    // Spawn Metrics & Health server
    let health = HealthState::new();
    let health_clone = health.clone();
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
                if let Err(e) = axum::serve(listener, app).await {
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
    manager.run().await
}
