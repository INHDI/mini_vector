use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use reqwest::Client;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::batcher::Batcher;
use crate::config::{BatchConfig, OpenSearchAuthConfig, OpenSearchBulkConfig, OpenSearchTlsConfig};
use crate::event::{Event, Value};
use crate::sinks::Sink;

#[derive(Clone)]
pub struct OpenSearchSink {
    pub name: String,
    pub endpoints: Vec<String>,
    pub _mode: String, // currently only "bulk" supported
    pub bulk: OpenSearchBulkConfig,
    pub auth: Option<OpenSearchAuthConfig>,
    pub _tls: Option<OpenSearchTlsConfig>, // used during client build
    pub batch_config: Option<BatchConfig>,
    pub client: Client,
}

impl OpenSearchSink {
    pub fn new(
        name: String,
        endpoints: Vec<String>,
        mode: String,
        bulk: OpenSearchBulkConfig,
        auth: Option<OpenSearchAuthConfig>,
        tls: Option<OpenSearchTlsConfig>,
        batch_config: Option<BatchConfig>,
    ) -> anyhow::Result<Self> {
        let mut builder = reqwest::ClientBuilder::new();

        if let Some(tls_cfg) = &tls {
            if !tls_cfg.verify_certificate {
                builder = builder.danger_accept_invalid_certs(true);
            }
            if !tls_cfg.verify_hostname {
                builder = builder.danger_accept_invalid_hostnames(true);
            }
        }

        let client = builder.build()?;

        Ok(Self {
            name,
            endpoints,
            _mode: mode,
            bulk,
            auth,
            _tls: tls,
            batch_config,
            client,
        })
    }

    fn resolve_index(&self) -> String {
        let now = Utc::now();
        now.format(&self.bulk.index).to_string()
    }

    fn value_to_json(v: &Value) -> serde_json::Value {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Integer(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => serde_json::json!(f),
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Timestamp(ts) => serde_json::Value::String(ts.to_rfc3339()),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(Self::value_to_json).collect())
            }
            Value::Object(map) => {
                let mut m = serde_json::Map::new();
                for (k, v) in map {
                    m.insert(k.clone(), Self::value_to_json(v));
                }
                serde_json::Value::Object(m)
            }
            Value::Bytes(b) => serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
        }
    }

    fn event_to_doc(event: Event) -> serde_json::Value {
        match event {
            Event::Log(log) => {
                let mut m = serde_json::Map::new();
                for (k, v) in log.fields {
                    let mut val = v;
                    // ensure timestamp string
                    if k == "@timestamp" {
                        if let Value::Timestamp(ts) = &val {
                            let s = ts.to_rfc3339();
                            val = Value::String(s);
                        }
                    }
                    m.insert(k, Self::value_to_json(&val));
                }
                serde_json::Value::Object(m)
            }
        }
    }

    async fn send_bulk(&self, events: Vec<Event>) {
        if events.is_empty() {
            return;
        }
        let index = self.resolve_index();
        let mut body = String::new();
        for ev in events {
            let header = serde_json::json!({ "index": { "_index": index } });
            let doc = Self::event_to_doc(ev);
            body.push_str(&serde_json::to_string(&header).unwrap_or_else(|_| "{}".into()));
            body.push('\n');
            body.push_str(&serde_json::to_string(&doc).unwrap_or_else(|_| "{}".into()));
            body.push('\n');
        }

        let endpoint = format!("{}/_bulk", self.endpoints[0].trim_end_matches('/'));
        let mut req = self.client.post(endpoint).header("Content-Type", "application/json");

        if let Some(auth) = &self.auth {
            if auth.strategy == "basic" {
                if let (Some(user), Some(pw)) = (&auth.user, &auth.password) {
                    req = req.basic_auth(user, Some(pw));
                }
            }
        }

        let res = req.body(body).send().await;
        match res {
            Ok(r) => {
                if !r.status().is_success() {
                    warn!(
                        "OpenSearchSink[{}] status={} for bulk request",
                        self.name,
                        r.status()
                    );
                } else if let Ok(txt) = r.text().await {
                    // naive check for errors flag
                    if txt.contains(r#""errors":true"#) {
                        warn!("OpenSearchSink[{}] bulk response has errors", self.name);
                    }
                }
            }
            Err(err) => {
                warn!(
                    "OpenSearchSink[{}] error sending bulk of events: {}",
                    self.name, err
                );
            }
        }
    }
}

#[async_trait]
impl Sink for OpenSearchSink {
    async fn run(self: Box<Self>, mut rx: mpsc::Receiver<Event>) {
        info!("OpenSearchSink[{}] started", self.name);
        let mut batcher = Batcher::new(self.batch_config.clone());

        loop {
            let timeout = batcher.remaining_time();
            tokio::select! {
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            batcher.add(event);
                            if batcher.should_flush() {
                                self.send_bulk(batcher.take()).await;
                            }
                        }
                        None => {
                            if !batcher.items.is_empty() {
                                self.send_bulk(batcher.take()).await;
                            }
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    if batcher.should_flush() {
                        self.send_bulk(batcher.take()).await;
                    }
                }
            }
        }

        info!("OpenSearchSink[{}] exiting", self.name);
    }
}

