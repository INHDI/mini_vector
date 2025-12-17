use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::event::{EventEnvelope, Value};
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
    async fn run(
        self: Box<Self>,
        mut input: mpsc::Receiver<EventEnvelope>,
        output: mpsc::Sender<EventEnvelope>,
    ) {
        while let Some(mut event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            
            event.event.insert(self.field.clone(), self.value.clone());
            
            match output.send(event).await {
                Ok(_) => {
                    metrics::increment_counter!("events_out", "component" => self.name.clone());
                }
                Err(err) => {
                    let ev = err.0;
                    ev.ack.ack();
                    break;
                }
            }
        }
    }
}
