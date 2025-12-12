use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{info, warn};
use metrics;

use crate::batcher::Batcher;
use crate::config::BatchConfig;
use crate::event::Event;
use crate::sinks::Sink;

pub struct HttpSink {
    pub name: String,
    pub endpoint: String,
    pub client: reqwest::Client,
    pub batch_config: Option<BatchConfig>,
}

impl HttpSink {
    pub fn new(name: String, endpoint: String, batch_config: Option<BatchConfig>) -> Self {
        Self {
            name,
            endpoint,
            client: reqwest::Client::new(),
            batch_config,
        }
    }

    async fn send_batch(&self, events: Vec<Event>) {
        let batch_len = events.len();
        if batch_len == 0 {
            return;
        }

        let body = serde_json::to_value(&events).unwrap_or_else(|_| serde_json::json!([]));

        let res = self.client.post(&self.endpoint).json(&body).send().await;

        match res {
            Ok(r) => {
                if r.status().is_success() {
                    metrics::counter!(
                        "events_out",
                        batch_len as u64,
                        "component" => self.name.clone()
                    );
                } else {
                    warn!(
                        "HttpSink[{}] status={} for batch of {} events",
                        self.name,
                        r.status(),
                        batch_len
                    );
                    metrics::counter!(
                        "events_failed",
                        batch_len as u64,
                        "component" => self.name.clone(),
                        "reason" => "http_status"
                    );
                }
            }
            Err(err) => {
                warn!(
                    "HttpSink[{}] error sending batch of {} events: {}",
                    self.name,
                    batch_len,
                    err
                );
                metrics::counter!(
                    "events_failed",
                    batch_len as u64,
                    "component" => self.name.clone(),
                    "reason" => "http_error"
                );
            }
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn run(self: Box<Self>, mut rx: mpsc::Receiver<Event>) {
        info!("HttpSink[{}] started, endpoint={}", self.name, self.endpoint);

        let mut batcher = Batcher::new(self.batch_config.clone());

        loop {
            let timeout = batcher.remaining_time();

            tokio::select! {
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
