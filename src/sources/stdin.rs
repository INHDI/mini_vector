use async_trait::async_trait;
use metrics;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::event::Event;
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
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>, mut shutdown: broadcast::Receiver<()>) {
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
                            if tx.send(event).await.is_err() {
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
                _ = shutdown.recv() => {
                    info!("StdinSource[{}] received shutdown signal", self.name);
                    break;
                }
            }
        }
        info!("StdinSource[{}] exiting", self.name);
    }
}
