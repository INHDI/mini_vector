use async_trait::async_trait;
use metrics;
use std::time::Duration;
use tracing::{info, warn};
use tokio_util::sync::CancellationToken;

use crate::batcher::{Batch, Batcher};
use crate::config::BatchConfig;
use crate::queue::SinkReceiver;
use crate::sinks::Sink;

pub struct HttpSink {
    pub name: String,
    pub endpoint: String,
    pub client: reqwest::Client,
    pub batch_config: Option<BatchConfig>,
    pub retry_attempts: u32,
    pub retry_backoff_secs: u64,
}

impl HttpSink {
    pub fn new(name: String, endpoint: String, batch_config: Option<BatchConfig>) -> Self {
        Self {
            name,
            endpoint,
            client: reqwest::Client::new(),
            batch_config,
            retry_attempts: 3,
            retry_backoff_secs: 1,
        }
    }

    async fn send_batch(&self, batch: Batch) {
        let batch_len = batch.events.len();
        if batch_len == 0 {
            return;
        }

        let body = serde_json::to_value(
            batch
                .events
                .iter()
                .map(|ev| &ev.event)
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| serde_json::json!([]));

        let mut attempt = 0;
        let mut backoff = self.retry_backoff_secs;
        let pending = batch.events;

        while attempt < self.retry_attempts {
            attempt += 1;
            let res = self.client.post(&self.endpoint).json(&body).send().await;

            match res {
                Ok(r) => {
                    if r.status().is_success() {
                        metrics::counter!(
                            "events_out",
                            pending.len() as u64,
                            "component" => self.name.clone()
                        );
                        metrics::increment_counter!("batches_sent", "component" => self.name.clone());
                        metrics::counter!(
                            "batch_size_event",
                            pending.len() as u64,
                            "component" => self.name.clone()
                        );
                        metrics::counter!(
                            "batch_size_bytes",
                            batch.bytes as u64,
                            "component" => self.name.clone()
                        );
                        for ev in pending {
                            ev.ack.ack();
                        }
                        return;
                    } else {
                        warn!(
                            "HttpSink[{}] status={} attempt={}/{} for batch of {} events",
                            self.name,
                            r.status(),
                            attempt,
                            self.retry_attempts,
                            pending.len()
                        );
                        metrics::counter!(
                            "events_failed",
                            pending.len() as u64,
                            "component" => self.name.clone(),
                            "reason" => "http_status"
                        );
                        metrics::increment_counter!("batches_failed", "component" => self.name.clone());
                    }
                }
                Err(err) => {
                    warn!(
                        "HttpSink[{}] error sending batch attempt={}/{}: {}",
                        self.name, attempt, self.retry_attempts, err
                    );
                    metrics::counter!(
                        "events_failed",
                        pending.len() as u64,
                        "component" => self.name.clone(),
                        "reason" => "http_error"
                    );
                    metrics::increment_counter!("batches_failed", "component" => self.name.clone());
                }
            }

            if attempt >= self.retry_attempts {
                break;
            }
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).max(1);
        }

        for ev in pending {
            ev.ack.ack();
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn run(self: Box<Self>, rx: SinkReceiver, shutdown: CancellationToken) {
        info!(
            "HttpSink[{}] started, endpoint={}",
            self.name, self.endpoint
        );

        let mut batcher = Batcher::new(self.batch_config.clone());

        loop {
            let timeout = batcher.remaining_time();

            tokio::select! {
                _ = shutdown.cancelled() => {
                    if batcher.should_flush() {
                        self.send_batch(batcher.take()).await;
                    }
                    break;
                }
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            metrics::increment_counter!("events_in", "component" => self.name.clone());
                            batcher.add(event);
                            if batcher.should_flush() {
                                self.send_batch(batcher.take()).await;
                            }
                        }
                        None => {
                            if !batcher.items.is_empty() {
                                self.send_batch(batcher.take()).await;
                            }
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    if batcher.should_flush() {
                        self.send_batch(batcher.take()).await;
                    }
                }
            }
        }

        info!("HttpSink[{}] exiting", self.name);
    }
}
