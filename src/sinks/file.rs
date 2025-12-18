use async_trait::async_trait;
use chrono::Utc;
use metrics;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::event::EventEnvelope;
use crate::queue::SinkReceiver;
use crate::sinks::Sink;

pub struct FileSink {
    pub name: String,
    pub path: String,
    pub max_bytes: u64,
    current_bytes: u64,
    writer: tokio::io::BufWriter<tokio::fs::File>,
}

impl FileSink {
    pub async fn new(name: String, path: String, max_bytes: u64) -> anyhow::Result<Self> {
        let (writer, current_bytes) = Self::open_writer(&path).await?;
        Ok(Self {
            name,
            path,
            max_bytes,
            current_bytes,
            writer,
        })
    }

    async fn open_writer(
        path: &str,
    ) -> anyhow::Result<(tokio::io::BufWriter<tokio::fs::File>, u64)> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let size = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        Ok((tokio::io::BufWriter::new(file), size))
    }

    async fn rotate(&mut self) -> anyhow::Result<()> {
        let ts = Utc::now().timestamp_millis();
        let rolled = format!("{}.{}", self.path, ts);
        self.writer.flush().await.ok();
        if let Err(err) = fs::rename(&self.path, &rolled).await {
            warn!("FileSink[{}] failed to rotate file: {}", self.name, err);
        }
        let (writer, current_bytes) = Self::open_writer(&self.path).await?;
        self.writer = writer;
        self.current_bytes = current_bytes;
        Ok(())
    }

    async fn write_event(&mut self, event: &EventEnvelope) -> anyhow::Result<()> {
        let line = serde_json::to_string(&event.event)?;
        let bytes = line.as_bytes();
        if self.current_bytes + bytes.len() as u64 + 1 > self.max_bytes {
            self.rotate().await?;
        }
        self.writer.write_all(bytes).await?;
        self.writer.write_all(b"\n").await?;
        self.current_bytes += bytes.len() as u64 + 1;
        Ok(())
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn run(mut self: Box<Self>, rx: SinkReceiver, shutdown: CancellationToken) {
        info!("FileSink[{}] writing to {}", self.name, self.path);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                maybe = rx.recv() => {
                    let Some(event) = maybe else { break };
                    metrics::increment_counter!("events_in", "component" => self.name.clone());
                    match self.write_event(&event).await {
                        Ok(()) => {
                            metrics::increment_counter!("events_out", "component" => self.name.clone());
                        }
                        Err(err) => {
                            error!("FileSink[{}] write error: {}", self.name, err);
                            metrics::increment_counter!(
                                "events_failed",
                                "component" => self.name.clone(),
                                "reason" => "write_error"
                            );
                        }
                    }
                    event.ack.ack();
                }
            }
        }
        if let Err(err) = self.writer.flush().await {
            warn!("FileSink[{}] flush error: {}", self.name, err);
        }
        info!("FileSink[{}] exiting", self.name);
    }
}
