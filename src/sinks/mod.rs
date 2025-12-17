use async_trait::async_trait;

use crate::queue::SinkReceiver;

pub mod console;
pub mod file;
pub mod http;
pub mod opensearch;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn run(self: Box<Self>, rx: SinkReceiver);
}
