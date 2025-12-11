use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use reqwest::Client;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::batcher::Batcher;
use crate::config::{
    BatchConfig, OpenSearchAuthConfig, OpenSearchBulkConfig, OpenSearchRetryConfig, OpenSearchTlsConfig,
};
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
    pub retry: OpenSearchRetryConfig,
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
        retry: Option<OpenSearchRetryConfig>,
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
            retry: retry.unwrap_or_else(|| OpenSearchRetryConfig {
                attempts: 3,
                backoff_secs: 1,
                max_backoff_secs: 3,
            }),
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

        let mut attempt = 0;
        let mut backoff = self.retry.backoff_secs;
        loop {
            attempt += 1;
            let res = req.try_clone().unwrap().body(body.clone()).send().await;
            let mut should_retry = false;

            match res {
                Ok(r) => {
                    let status_ok = r.status().is_success();
                    let status_code = r.status();
                    let txt = r.text().await.unwrap_or_else(|_| "".to_string());
                    let has_errors = txt.contains(r#""errors":true"#);
                    if !status_ok || has_errors {
                        warn!(
                            "OpenSearchSink[{}] bulk status={} errors_flag={} attempt={}/{}",
                            self.name,
                            status_code,
                            has_errors,
                            attempt,
                            self.retry.attempts
                        );
                        should_retry = attempt < self.retry.attempts;
                    }
                }
                Err(err) => {
                    warn!(
                        "OpenSearchSink[{}] error sending bulk attempt={}/{}: {}",
                        self.name,
                        attempt,
                        self.retry.attempts,
                        err
                    );
                    should_retry = attempt < self.retry.attempts;
                }
            }

            if should_retry {
                let sleep_dur = std::time::Duration::from_secs(backoff);
                tokio::time::sleep(sleep_dur).await;
                backoff = (backoff * 2).min(self.retry.max_backoff_secs);
                continue;
            }
            break;
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

