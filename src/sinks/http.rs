use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::event::Event;
use crate::sinks::Sink;

pub struct HttpSink {
    pub name: String,
    pub endpoint: String,
    pub client: reqwest::Client,
}

impl HttpSink {
    pub fn new(name: String, endpoint: String) -> Self {
        Self {
            name,
            endpoint,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn run(self: Box<Self>, mut rx: mpsc::Receiver<Event>) {
        info!("HttpSink[{}] started, endpoint={}", self.name, self.endpoint);

        while let Some(event) = rx.recv().await {
            let body =
                serde_json::to_value(&event).unwrap_or_else(|_| serde_json::json!({}));

            let res = self.client.post(&self.endpoint).json(&body).send().await;

            match res {
                Ok(r) => {
                    if !r.status().is_success() {
                        warn!(
                            "HttpSink[{}] status={} for event",
                            self.name,
                            r.status()
                        );
                    }
                }
                Err(err) => {
                    warn!("HttpSink[{}] error sending event: {}", self.name, err);
                }
            }
        }

        info!("HttpSink[{}] exiting", self.name);
    }
}
