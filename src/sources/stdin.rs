use async_trait::async_trait;
use metrics;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::event::{Event, EventEnvelope};
use crate::sources::Source;

pub struct StdinSource {
    pub name: String,
}

impl StdinSource {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl Source for StdinSource {
    async fn run(
        self: Box<Self>,
        tx: mpsc::Sender<EventEnvelope>,
        shutdown: CancellationToken,
    ) {
        use tokio::io::{self, AsyncBufReadExt};

        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin).lines();

        info!("StdinSource[{}] started", self.name);
        loop {
            tokio::select! {
                res = reader.next_line() => {
                    match res {
                        Ok(Some(line)) => {
                            let mut event = Event::new();
                            event.insert("message", line);
                            let envelope = EventEnvelope::with_source(event, self.name.clone());
                            if let Err(err) = tx.send(envelope).await {
                                err.0.ack.ack();
                                break;
                            }
                            metrics::increment_counter!("events_out", "component" => self.name.clone());
                        }
                        Ok(None) => break, // EOF
                        Err(err) => {
                            warn!("StdinSource[{}] read error: {}", self.name, err);
                            metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "read_error");
                            break;
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("StdinSource[{}] received shutdown signal", self.name);
                    break;
                }
            }
        }
        info!("StdinSource[{}] exiting", self.name);
    }
}
