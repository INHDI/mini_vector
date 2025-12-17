use std::fs::File;
use std::path::PathBuf;
use std::time::Duration;

use axum::{Router, routing::post};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::FullConfig;
use crate::health::HealthState;
use crate::pipeline::run_pipeline;

pub struct PipelineManager {
    config_path: PathBuf,
    reload_tx: mpsc::Sender<()>,
    reload_rx: mpsc::Receiver<()>,
    current: Option<RunningPipeline>,
    health: HealthState,
}

struct RunningPipeline {
    shutdown: broadcast::Sender<()>,
    handle: JoinHandle<anyhow::Result<()>>,
}

impl PipelineManager {
    pub fn new(config_path: PathBuf, health: HealthState) -> Self {
        let (tx, rx) = mpsc::channel(8);
        Self {
            config_path,
            reload_tx: tx,
            reload_rx: rx,
            current: None,
            health,
        }
    }

    fn load_config(&self) -> anyhow::Result<FullConfig> {
        let file = File::open(&self.config_path)?;
        let cfg: FullConfig = serde_yaml::from_reader(file)?;
        cfg.validate()?;
        Ok(cfg)
    }

    async fn start_pipeline(&mut self, cfg: FullConfig) -> anyhow::Result<()> {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(4);
        let health_clone = self.health.clone();
        let handle = tokio::spawn(async move {
            health_clone.mark_config_ok();
            run_pipeline(cfg, Some(shutdown_rx), health_clone.clone()).await
        });
        self.current = Some(RunningPipeline {
            shutdown: shutdown_tx,
            handle,
        });
        Ok(())
    }

    async fn stop_pipeline(old: RunningPipeline) {
        let _ = old.shutdown.send(());
        match tokio::time::timeout(Duration::from_secs(10), old.handle).await {
            Ok(res) => {
                if let Err(err) = res {
                    warn!("old pipeline join error: {}", err);
                }
            }
            Err(_) => {
                warn!("timeout draining old pipeline");
            }
        }
    }

    pub async fn reload(&mut self) {
        info!(target = "reload", "config_reload_started");
        let cfg = match self.load_config() {
            Ok(c) => c,
            Err(err) => {
                error!(target = "reload", %err, "config_reload_failed");
                return;
            }
        };

        let old = self.current.take();
        if let Err(err) = self.start_pipeline(cfg).await {
            error!(target = "reload", %err, "config_reload_failed");
            // restore old if failed to start new pipeline
            if let Some(old_pipe) = old {
                self.current = Some(old_pipe);
            }
            return;
        }

        if let Some(old_pipe) = old {
            Self::stop_pipeline(old_pipe).await;
        }
        info!(target = "reload", "config_reload_succeeded");
    }

    async fn handle_sighup(reload_tx: mpsc::Sender<()>) {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            if let Ok(mut stream) = signal(SignalKind::hangup()) {
                while stream.recv().await.is_some() {
                    let _ = reload_tx.send(()).await;
                }
            }
        }
    }

    async fn spawn_http_reload(reload_tx: mpsc::Sender<()>) {
        let port = std::env::var("RELOAD_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(9400);

        let tx = reload_tx.clone();
        let app = Router::new().route(
            "/reload",
            post(move || {
                let tx_inner = tx.clone();
                async move {
                    let _ = tx_inner.send(()).await;
                    "reload triggered"
                }
            }),
        );

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        info!(target = "reload", "reload endpoint listening on {}", addr);
        if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
            if let Err(err) = axum::serve(listener, app).await {
                warn!(target = "reload", "reload HTTP server error: {}", err);
            }
        } else {
            warn!(target = "reload", "failed to bind reload port {}", port);
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.reload().await; // initial start

        let tx_for_signal = self.reload_tx.clone();
        tokio::spawn(Self::handle_sighup(tx_for_signal));

        let tx_for_http = self.reload_tx.clone();
        tokio::spawn(Self::spawn_http_reload(tx_for_http));

        while self.reload_rx.recv().await.is_some() {
            self.reload().await;
        }

        Ok(())
    }
}
