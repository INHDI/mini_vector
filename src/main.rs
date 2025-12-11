mod config;
mod event;
mod pipeline;
mod sinks;
mod sources;
mod transforms;

use std::fs::File;
use std::path::PathBuf;

use tracing::{info, warn};

use crate::config::FullConfig;
use crate::pipeline::run_pipeline;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    // Initialize Prometheus metrics (listen on port 9100 by default for scraping)
    // We catch the error in case the port is busy, but we don't crash the app (maybe just log warning)
    match metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9100))
        .install()
    {
        Ok(_) => info!("Prometheus metrics listening on 0.0.0.0:9100"),
        Err(e) => warn!("Failed to install Prometheus recorder: {}", e),
    }

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("mini_vector.yml"));

    let file = File::open(&config_path)?;
    let config: FullConfig = serde_yaml::from_reader(file)?;
    config.validate()?;

    info!("Loaded config from {:?}", config_path);

    run_pipeline(config).await
}
