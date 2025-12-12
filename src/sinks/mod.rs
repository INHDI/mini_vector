use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::event::Event;

pub mod console;
pub mod file;
pub mod http;
pub mod opensearch;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn run(self: Box<Self>, rx: mpsc::Receiver<Event>);
}
