use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::event::EventEnvelope;

pub mod file;
pub mod http;
pub mod stdin;
pub mod syslog;
pub mod tcp;

#[async_trait]
pub trait Source: Send + Sync {
    async fn run(
        self: Box<Self>,
        tx: mpsc::Sender<EventEnvelope>,
        shutdown: CancellationToken,
    );
}
