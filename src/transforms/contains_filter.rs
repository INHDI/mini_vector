use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::event::Event;
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
    async fn run(self: Box<Self>, mut input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>) {
        while let Some(event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            
            let keep = if let Some(v) = event.get_str(&self.field) {
                v.contains(&self.needle)
            } else {
                false
            };

            if keep {
                if output.send(event).await.is_err() {
                    break;
                }
                metrics::increment_counter!("events_out", "component" => self.name.clone());
            } else {
                metrics::increment_counter!(
                    "events_dropped",
                    "component" => self.name.clone(),
                    "reason" => "filtered_out"
                );
            }
        }
    }
}
