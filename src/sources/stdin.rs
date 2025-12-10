use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

use crate::event::Event;
use crate::sources::Source;

pub struct StdinSource;

#[async_trait]
impl Source for StdinSource {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>) {
        use tokio::io::{self, AsyncBufReadExt};

        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin).lines();

        info!("StdinSource started");
        while let Ok(Some(line)) = reader.next_line().await {
            let mut event = Event::new();
            event.insert("message", line);
            if tx.send(event).await.is_err() {
                break;
            }
        }
        info!("StdinSource exiting");
    }
}
