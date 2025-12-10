use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

use crate::event::Event;
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
    async fn run(self: Box<Self>, mut rx: mpsc::Receiver<Event>) {
        info!("ConsoleSink[{}] started", self.name);
        while let Some(event) = rx.recv().await {
            println!(
                "[sink:{}] {}",
                self.name,
                serde_json::to_string(&event).unwrap()
            );
        }
        info!("ConsoleSink[{}] exiting", self.name);
    }
}
