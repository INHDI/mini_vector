use std::net::SocketAddr;

use async_trait::async_trait;
use axum::{
    Router as HttpRouter,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use bytes::Bytes;
use metrics;
use serde_json::Value as JsonValue;
use tokio::sync::{broadcast, mpsc};
use tower_http::decompression::RequestDecompressionLayer;
use tracing::{info, warn};

use crate::event::{Event, EventEnvelope, value_from_json};
use crate::sources::Source;

#[derive(Clone)]
pub struct HttpSourceState {
    tx: mpsc::Sender<EventEnvelope>,
    name: String,
}

pub struct HttpSource {
    name: String,
    addr: SocketAddr,
    path: String,
}

impl HttpSource {
    pub fn new(name: String, addr: SocketAddr, path: String) -> Self {
        Self { name, addr, path }
    }
}

async fn send_object(
    component: &str,
    obj: serde_json::Map<String, JsonValue>,
    tx: &mpsc::Sender<EventEnvelope>,
) {
    let mut event = Event::new();
    for (k, v) in obj {
        event.insert(k, value_from_json(&v));
    }
    let envelope = EventEnvelope::with_source(event, component.to_string());
    if let Err(err) = tx.send(envelope).await {
        warn!("HttpSource: failed to send event: {}", err);
    } else {
        metrics::increment_counter!("events_out", "component" => component.to_string());
    }
}

async fn send_message(component: &str, msg: String, tx: &mpsc::Sender<EventEnvelope>) {
    let mut event = Event::new();
    event.insert("message", msg);
    let envelope = EventEnvelope::with_source(event, component.to_string());
    if let Err(err) = tx.send(envelope).await {
        warn!("HttpSource: failed to send message event: {}", err);
    } else {
        metrics::increment_counter!("events_out", "component" => component.to_string());
    }
}

async fn handle_json_value(component: &str, val: JsonValue, tx: &mpsc::Sender<EventEnvelope>) {
    match val {
        JsonValue::Object(map) => send_object(component, map, tx).await,
        JsonValue::Array(arr) => {
            for item in arr {
                if let JsonValue::Object(map) = item {
                    send_object(component, map, tx).await;
                } else {
                    send_message(component, item.to_string(), tx).await;
                }
            }
        }
        other => send_message(component, other.to_string(), tx).await,
    }
}

async fn parse_ndjson(component: &str, body: &str, tx: &mpsc::Sender<EventEnvelope>) {
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<JsonValue>(trimmed) {
            Ok(JsonValue::Object(map)) => send_object(component, map, tx).await,
            Ok(other) => send_message(component, other.to_string(), tx).await,
            Err(err) => {
                warn!("HttpSource: failed to parse NDJSON line: {}", err);
                metrics::increment_counter!("events_dropped", "component" => component.to_string(), "reason" => "read_error");
                send_message(component, trimmed.to_string(), tx).await;
            }
        }
    }
}

async fn http_source_handler(
    State(state): State<HttpSourceState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    // Try JSON first when content-type is application/json
    if content_type.starts_with("application/json") {
        match serde_json::from_slice::<JsonValue>(&body) {
            Ok(val) => {
                handle_json_value(&state.name, val, &state.tx).await;
                return StatusCode::OK;
            }
            Err(err) => {
                warn!("HttpSource: failed to parse JSON body: {}", err);
                // fall through to NDJSON/plain parsing
            }
        }
    }

    // NDJSON or text/plain: treat as newline-delimited JSON/text
    if content_type.contains("application/x-ndjson")
        || content_type.contains("application/json")
        || content_type.contains("text/plain")
        || content_type.is_empty()
    {
        match String::from_utf8(body.to_vec()) {
            Ok(s) => {
                parse_ndjson(&state.name, &s, &state.tx).await;
                return StatusCode::OK;
            }
            Err(err) => {
                warn!("HttpSource: body is not valid UTF-8: {}", err);
                let msg = format!("{:?}", body);
                send_message(&state.name, msg, &state.tx).await;
                return StatusCode::OK;
            }
        }
    }

    // Fallback: raw bytes to string
    let msg = String::from_utf8_lossy(&body).to_string();
    send_message(&state.name, msg, &state.tx).await;
    StatusCode::OK
}

#[async_trait]
impl Source for HttpSource {
    async fn run(
        self: Box<Self>,
        tx: mpsc::Sender<EventEnvelope>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        info!(
            "HttpSource[{}] listening on http://{}{}",
            self.name, self.addr, self.path
        );

        let state = HttpSourceState {
            tx,
            name: self.name.clone(),
        };
        let app = HttpRouter::new()
            .route(&self.path, post(http_source_handler))
            .layer(RequestDecompressionLayer::new())
            .with_state(state);

        let listener = match tokio::net::TcpListener::bind(self.addr).await {
            Ok(l) => l,
            Err(err) => {
                warn!("HttpSource: failed to bind address {}: {}", self.addr, err);
                return;
            }
        };

        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = shutdown.recv().await;
            info!("HttpSource received shutdown signal");
        });

        if let Err(err) = server.await {
            warn!("HttpSource error: {}", err);
        }

        info!("HttpSource exiting");
    }
}
