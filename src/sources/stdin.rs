use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

use crate::event::Event;
use crate::sources::Source;

pub struct StdinSource;

#[async_trait]
impl Source for StdinSource {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>, mut shutdown: broadcast::Receiver<()>) {
        use tokio::io::{self, AsyncBufReadExt};

        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin).lines();

        info!("StdinSource started");
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
                        }
                        _ => break, // EOF or error
                    }
                }
                _ = shutdown.recv() => {
                    info!("StdinSource received shutdown signal");
                    break;
                }
            }
        }
        info!("StdinSource exiting");
    }
}
