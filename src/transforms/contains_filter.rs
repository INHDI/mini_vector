use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::event::EventEnvelope;
use crate::transforms::Transform;

pub struct ContainsFilterTransform {
    pub name: String,
    pub field: String,
    pub needle: String,
}

impl ContainsFilterTransform {
    pub fn new(name: String, field: String, needle: String) -> Self {
        Self { name, field, needle }
    }
}

#[async_trait]
impl Transform for ContainsFilterTransform {
    async fn run(
        self: Box<Self>,
        mut input: mpsc::Receiver<EventEnvelope>,
        output: mpsc::Sender<EventEnvelope>,
    ) {
        while let Some(event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            
            let keep = if let Some(v) = event.event.get_str(&self.field) {
                v.contains(&self.needle)
            } else {
                false
            };

            if keep {
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
            } else {
                metrics::increment_counter!(
                    "events_dropped",
                    "component" => self.name.clone(),
                    "reason" => "filtered_out"
                );
                event.ack.ack();
            }
        }
    }
}
