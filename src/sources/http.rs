use std::net::SocketAddr;

use async_trait::async_trait;
use axum::{extract::State, routing::post, Json, Router as HttpRouter};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::event::{Event, Value};
use crate::sources::Source;

#[derive(Clone)]
pub struct HttpSourceState {
    tx: mpsc::Sender<Event>,
}

pub struct HttpSource {
    addr: SocketAddr,
    path: String,
}

impl HttpSource {
    pub fn new(addr: SocketAddr, path: String) -> Self {
        Self { addr, path }
    }
}

async fn http_source_handler(State(state): State<HttpSourceState>, Json(body): Json<JsonValue>) {
    let mut event = Event::new();

    match body {
        JsonValue::Object(map) => {
            for (k, v) in map {
                let v = match v {
                    JsonValue::String(s) => Value::String(s),
                    JsonValue::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Value::Integer(i)
                        } else if let Some(f) = n.as_f64() {
                            Value::Float(f)
                        } else {
                            Value::String(n.to_string())
                        }
                    }
                    JsonValue::Bool(b) => Value::Bool(b),
                    _ => Value::String(v.to_string()),
                };
                event.insert(k, v);
            }
        }
        other => {
            event.insert("message", other.to_string());
        }
    }

    if let Err(err) = state.tx.send(event).await {
        warn!("HttpSource: failed to send event to pipeline: {}", err);
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>) {
        info!(
            "HttpSource listening on http://{}{}",
            self.addr, self.path
        );

        let state = HttpSourceState { tx };
        let app = HttpRouter::new()
            .route(&self.path, post(http_source_handler))
            .with_state(state);

        let listener = match tokio::net::TcpListener::bind(self.addr).await {
            Ok(l) => l,
            Err(err) => {
                warn!(
                    "HttpSource: failed to bind address {}: {}",
                    self.addr, err
                );
                return;
            }
        };

        if let Err(err) = axum::serve(listener, app).await {
            warn!("HttpSource error: {}", err);
        }

        info!("HttpSource exiting");
    }
}
