use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::event::Event;

pub mod add_field;
pub mod contains_filter;
pub mod json_parse;
pub mod normalize_schema;
pub mod regex_parse;
pub mod script;
pub mod remap;

#[async_trait]
pub trait Transform: Send + Sync {
    async fn run(self: Box<Self>, input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>);
}
