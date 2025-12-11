use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::event::{Event, Value};
use crate::transforms::Transform;

pub struct AddFieldTransform {
    pub name: String,
    pub field: String,
    pub value: Value,
}

impl AddFieldTransform {
    pub fn new(name: String, field: String, value: Value) -> Self {
        Self { name, field, value }
    }
}

#[async_trait]
impl Transform for AddFieldTransform {
    async fn run(self: Box<Self>, mut input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>) {
        while let Some(mut event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            
            event.insert(self.field.clone(), self.value.clone());
            
            if output.send(event).await.is_err() {
                break;
            }
            metrics::increment_counter!("events_out", "component" => self.name.clone());
        }
    }
}
