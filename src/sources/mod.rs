use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};

use crate::event::Event;

pub mod file;
pub mod http;
pub mod stdin;

#[async_trait]
pub trait Source: Send + Sync {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>, shutdown: broadcast::Receiver<()>);
}
