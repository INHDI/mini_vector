use async_trait::async_trait;
use metrics;
use tracing::{error, info};

use crate::queue::SinkReceiver;
use crate::sinks::Sink;
use tokio_util::sync::CancellationToken;

pub struct ConsoleSink {
    pub name: String,
}

impl ConsoleSink {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    async fn run(self: Box<Self>, rx: SinkReceiver, shutdown: CancellationToken) {
        info!("ConsoleSink[{}] started", self.name);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                maybe = rx.recv() => {
                    let Some(envelope) = maybe else { break };
                    metrics::increment_counter!("events_in", "component" => self.name.clone());

                    match serde_json::to_string(&envelope.event) {
                        Ok(serialized) => {
                            println!("[sink:{}] {}", self.name, serialized);
                            metrics::increment_counter!("events_out", "component" => self.name.clone());
                        }
                        Err(err) => {
                            error!(
                                "ConsoleSink[{}] failed to serialize event: {}",
                                self.name, err
                            );
                            metrics::increment_counter!(
                                "events_failed",
                                "component" => self.name.clone(),
                                "reason" => "serialize_error"
                            );
                        }
                    }
                    envelope.ack.ack();
                }
            }
        }
        info!("ConsoleSink[{}] exiting", self.name);
    }
}
