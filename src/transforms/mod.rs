use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::event::EventEnvelope;

pub mod add_field;
pub mod contains_filter;
pub mod detect;
pub mod json_parse;
pub mod normalize_schema;
pub mod regex_parse;
pub mod remap;
pub mod route;
pub mod script;

#[async_trait]
pub trait Transform: Send + Sync {
    async fn run(
        self: Box<Self>,
        input: mpsc::Receiver<EventEnvelope>,
        output: mpsc::Sender<EventEnvelope>,
    );
}
