use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use reqwest::Client;
use tokio::time::Duration;
use tracing::{info, warn};
use metrics;
use std::collections::HashSet;

use crate::batcher::{Batch, Batcher};
use crate::config::{
    BatchConfig, OpenSearchAuthConfig, OpenSearchBulkConfig, OpenSearchRetryConfig, OpenSearchTlsConfig,
};
use crate::event::{Event, EventEnvelope, Value};
use crate::queue::SinkReceiver;
use crate::sinks::Sink;
use crate::health::HealthState;

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
    pub health: Option<HealthState>,
}

struct BulkAttemptResult {
    success: Vec<usize>,
    retryable: Vec<usize>,
    fatal: Vec<usize>,
    had_errors: bool,
    status: Option<reqwest::StatusCode>,
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
        health: Option<HealthState>,
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
            health,
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

    fn build_bulk_body(&self, events: &[EventEnvelope]) -> String {
        let index = self.resolve_index();
        let mut body = String::new();
        for ev in events {
            let header = serde_json::json!({ "index": { "_index": index } });
            let doc = Self::event_to_doc(ev.event.clone());
            body.push_str(&serde_json::to_string(&header).unwrap_or_else(|_| "{}".into()));
            body.push('\n');
            body.push_str(&serde_json::to_string(&doc).unwrap_or_else(|_| "{}".into()));
            body.push('\n');
        }
        body
    }

    async fn send_bulk_once(
        &self,
        events: &[EventEnvelope],
        body: &str,
    ) -> BulkAttemptResult {
        let mut result = BulkAttemptResult {
            success: Vec::new(),
            retryable: Vec::new(),
            fatal: Vec::new(),
            had_errors: false,
            status: None,
        };

        let endpoint = format!("{}/_bulk", self.endpoints[0].trim_end_matches('/'));
        let mut base_req = self
            .client
            .post(endpoint)
            .header("Content-Type", "application/json");

        if let Some(auth) = &self.auth {
            if auth.strategy == "basic" {
                if let (Some(user), Some(pw)) = (&auth.user, &auth.password) {
                    base_req = base_req.basic_auth(user, Some(pw));
                }
            }
        }

        let req = match base_req.try_clone() {
            Some(r) => r.body(body.to_string()),
            None => {
                result.fatal = (0..events.len()).collect();
                return result;
            }
        };

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                result.status = Some(status);
                let text = resp.text().await.unwrap_or_default();
                if !status.is_success() {
                    if status.is_server_error() {
                        result.retryable = (0..events.len()).collect();
                    } else {
                        result.fatal = (0..events.len()).collect();
                    }
                    return result;
                }

                let parsed: serde_json::Value =
                    serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({}));
                let has_errors = parsed
                    .get("errors")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                result.had_errors = has_errors;

                let items = parsed
                    .get("items")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                for (idx, item) in items.iter().enumerate() {
                    let status = item
                        .get("index")
                        .and_then(|i| i.get("status"))
                        .and_then(|s| s.as_u64())
                        .unwrap_or(500);
                    if (200..300).contains(&status) {
                        result.success.push(idx);
                    } else if status == 429 || status >= 500 {
                        result.retryable.push(idx);
                    } else {
                        result.fatal.push(idx);
                    }
                }

                if items.len() < events.len() {
                    result.retryable
                        .extend(items.len()..events.len());
                }
            }
            Err(err) => {
                let recoverable = err.is_timeout() || err.is_connect();
                if recoverable {
                    result.retryable = (0..events.len()).collect();
                } else {
                    result.fatal = (0..events.len()).collect();
                }
            }
        }

        result
    }

    async fn send_bulk_with_retry(&self, batch: Batch) {
        let mut pending = batch.events;
        if pending.is_empty() {
            return;
        }
        let mut attempt = 0;
        let mut backoff = self.retry.backoff_secs;
        let mut last_result: Option<BulkAttemptResult> = None;

        while !pending.is_empty() {
            attempt += 1;
            let body = self.build_bulk_body(&pending);
            let result = self.send_bulk_once(&pending, &body).await;
            let success: HashSet<_> = result.success.iter().copied().collect();
            let retryable: HashSet<_> = result.retryable.iter().copied().collect();
            let fatal: HashSet<_> = result.fatal.iter().copied().collect();

            if !retryable.is_empty() || !fatal.is_empty() || result.had_errors {
                warn!(
                    "OpenSearchSink[{}] bulk attempt={}/{} status={:?} success={} retryable={} fatal={}",
                    self.name,
                    attempt,
                    self.retry.attempts,
                    result.status,
                    success.len(),
                    retryable.len(),
                    fatal.len()
                );
            }

            let mut next_pending = Vec::new();
            for (idx, ev) in pending.into_iter().enumerate() {
                if success.contains(&idx) {
                    metrics::increment_counter!("events_out", "component" => self.name.clone());
                    ev.ack.ack();
                } else if fatal.contains(&idx) {
                    metrics::increment_counter!(
                        "events_failed",
                        "component" => self.name.clone(),
                        "reason" => "mapping_error"
                    );
                    ev.ack.ack();
                } else if retryable.contains(&idx) {
                    next_pending.push(ev);
                } else {
                    // default to retry if classification missing
                    next_pending.push(ev);
                }
            }

            if next_pending.is_empty() {
                if result.fatal.is_empty() && result.retryable.is_empty() {
                    if let Some(h) = &self.health {
                        h.mark_opensearch_ok();
                    }
                    metrics::increment_counter!("batches_sent", "component" => self.name.clone());
                    metrics::counter!(
                        "batch_size_event",
                        success.len() as u64,
                        "component" => self.name.clone()
                    );
                    metrics::counter!(
                        "batch_size_bytes",
                        batch.bytes as u64,
                        "component" => self.name.clone()
                    );
                } else {
                    metrics::increment_counter!("batches_failed", "component" => self.name.clone());
                }
                last_result = Some(result);
                break;
            }

            if attempt >= self.retry.attempts {
                metrics::increment_counter!("batches_failed", "component" => self.name.clone());
                for ev in next_pending {
                    metrics::increment_counter!(
                        "events_failed",
                        "component" => self.name.clone(),
                        "reason" => "retries_exhausted"
                    );
                    ev.ack.ack();
                }
                return;
            }

            pending = next_pending;
            last_result = Some(result);
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(self.retry.max_backoff_secs);
        }

        if let Some(res) = last_result {
            if res.had_errors || res.status.map(|s| s.is_client_error()).unwrap_or(false) {
                metrics::increment_counter!("batches_failed", "component" => self.name.clone());
            }
        }
    }
}

#[async_trait]
impl Sink for OpenSearchSink {
    async fn run(self: Box<Self>, rx: SinkReceiver) {
        info!("OpenSearchSink[{}] started", self.name);
        let mut batcher = Batcher::new(self.batch_config.clone());

        loop {
            let timeout = batcher.remaining_time();
            tokio::select! {
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            metrics::increment_counter!("events_in", "component" => self.name.clone());
                            batcher.add(event);
                            if batcher.should_flush() {
                                self.send_bulk_with_retry(batcher.take()).await;
                            }
                        }
                        None => {
                            if !batcher.items.is_empty() {
                                self.send_bulk_with_retry(batcher.take()).await;
                            }
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    if batcher.should_flush() {
                        self.send_bulk_with_retry(batcher.take()).await;
                    }
                }
            }
        }

        info!("OpenSearchSink[{}] exiting", self.name);
    }
}
