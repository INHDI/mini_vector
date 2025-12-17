use async_trait::async_trait;
use tracing::{info, error};
use metrics;

use crate::queue::SinkReceiver;
use crate::sinks::Sink;

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
    async fn run(self: Box<Self>, rx: SinkReceiver) {
        info!("ConsoleSink[{}] started", self.name);
        while let Some(envelope) = rx.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());

            match serde_json::to_string(&envelope.event) {
                Ok(serialized) => {
                    println!("[sink:{}] {}", self.name, serialized);
                    metrics::increment_counter!("events_out", "component" => self.name.clone());
                }
                Err(err) => {
                    error!("ConsoleSink[{}] failed to serialize event: {}", self.name, err);
                    metrics::increment_counter!(
                        "events_failed",
                        "component" => self.name.clone(),
                        "reason" => "serialize_error"
                    );
                }
            }
            envelope.ack.ack();
        }
        info!("ConsoleSink[{}] exiting", self.name);
    }
}
