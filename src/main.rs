mod config;
mod event;
mod pipeline;
mod router;
mod sinks;
mod sources;
mod transforms;

use std::fs::File;
use std::path::PathBuf;

use tracing::info;

use crate::config::FullConfig;
use crate::pipeline::run_pipeline;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("mini_vector.yml"));

    let file = File::open(&config_path)?;
    let config: FullConfig = serde_yaml::from_reader(file)?;

    info!("Loaded config from {:?}", config_path);

    run_pipeline(config).await
}
