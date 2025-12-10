use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{info, warn};

use std::net::SocketAddr;
use axum::{
    routing::post,
    Router as HttpRouter,
    extract::State,
    Json,
};
use serde_json::Value as JsonValue;

/// =========================
///  Event model
/// =========================

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

async fn http_source_handler(
    State(state): State<HttpSourceState>,
    Json(body): Json<JsonValue>,
) {
    // body là JSON, ta map sang Event.fields
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
                    return; // dừng source này, nhưng không kill cả process
                }
            };

        if let Err(err) = axum::serve(listener, app).await {
            warn!("HttpSource error: {}", err);
        }

        info!("HttpSource exiting");
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub fields: HashMap<String, Value>,
}

impl Event {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn insert<S: Into<String>, V: Into<Value>>(&mut self, key: S, value: V) {
        self.fields.insert(key.into(), value.into());
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.fields.get(key) {
            Some(Value::String(s)) => Some(s.as_str()),
            _ => None,
        }
    }
}

// Convenience conversions
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

/// =========================
///  Traits: Source / Transform / Sink
/// =========================

#[async_trait]
pub trait Source: Send + Sync {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>);
}

pub trait Transform: Send + Sync {
    /// Trả về false nếu muốn drop event.
    fn apply(&self, event: &mut Event) -> bool;
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn run(self: Box<Self>, rx: mpsc::Receiver<Event>);
}

/// =========================
///  Implementations: Sources
/// =========================

pub struct StdinSource;

#[async_trait]
impl Source for StdinSource {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>) {
        use tokio::io::{self, AsyncBufReadExt};

        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin).lines();

        info!("StdinSource started");
        while let Ok(Some(line)) = reader.next_line().await {
            let mut event = Event::new();
            event.insert("message", line);
            // Ở đây có thể parse thêm timestamp, host,...
            if tx.send(event).await.is_err() {
                warn!("StdinSource: receiver closed, stopping");
                break;
            }
        }
        info!("StdinSource exiting");
    }
}

/// =========================
///  Implementations: Transforms
/// =========================

pub struct AddFieldTransform {
    field: String,
    value: Value,
}

impl AddFieldTransform {
    pub fn new(field: String, value: Value) -> Self {
        Self { field, value }
    }
}

impl Transform for AddFieldTransform {
    fn apply(&self, event: &mut Event) -> bool {
        event.insert(self.field.clone(), self.value.clone());
        true
    }
}

/// Ví dụ transform filter theo substring
pub struct ContainsFilterTransform {
    field: String,
    needle: String,
}

impl ContainsFilterTransform {
    pub fn new(field: String, needle: String) -> Self {
        Self { field, needle }
    }
}

impl Transform for ContainsFilterTransform {
    fn apply(&self, event: &mut Event) -> bool {
        if let Some(v) = event.get_str(&self.field) {
            v.contains(&self.needle)
        } else {
            false
        }
    }
}

pub struct JsonParseTransform {
    from_field: String,
}

impl JsonParseTransform {
    pub fn new(from_field: String) -> Self {
        Self { from_field }
    }
}

impl Transform for JsonParseTransform {
    fn apply(&self, event: &mut Event) -> bool {
        // lấy chuỗi từ field cần parse
        let raw = match event.fields.get(&self.from_field) {
            Some(Value::String(s)) => s.clone(),
            _ => return true, // không có field / không phải string -> bỏ qua
        };

        match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(serde_json::Value::Object(map)) => {
                for (k, v) in map {
                    let v = match v {
                        serde_json::Value::String(s) => Value::String(s),
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                Value::Integer(i)
                            } else if let Some(f) = n.as_f64() {
                                Value::Float(f)
                            } else {
                                Value::String(n.to_string())
                            }
                        }
                        serde_json::Value::Bool(b) => Value::Bool(b),
                        other => Value::String(other.to_string()),
                    };
                    event.insert(k, v);
                }
                true
            }
            _ => {
                warn!(
                    "JsonParseTransform: failed to parse '{}' as JSON",
                    self.from_field
                );
                true // giữ nguyên event
            }
        }
    }
}


/// =========================
///  Implementations: Sinks
/// =========================

pub struct HttpSink {
    name: String,
    endpoint: String,
    client: reqwest::Client,
}

pub struct ConsoleSink {
    name: String,
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
            // Đơn giản: gửi từng event một (sau này có thể batch)
            let body = serde_json::to_value(&event).unwrap_or_else(|_| serde_json::json!({}));

            let res = self
                .client
                .post(&self.endpoint)
                .json(&body)
                .send()
                .await;

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

impl ConsoleSink {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    async fn run(self: Box<Self>, mut rx: mpsc::Receiver<Event>) {
        info!("ConsoleSink[{}] started", self.name);
        while let Some(event) = rx.recv().await {
            println!("[sink:{}] {}", self.name, serde_json::to_string(&event).unwrap());
        }
        info!("ConsoleSink[{}] exiting", self.name);
    }
}

/// =========================
///  Router
/// =========================

#[derive(Debug, Clone, Deserialize)]
pub struct RouteRuleConfig {
    pub equals: String,
    pub sink: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouterConfig {
    pub field: String,
    pub routes: Vec<RouteRuleConfig>,
}

pub struct EventRouter {
    field: String,
    rules: Vec<RouteRuleConfig>,
}

impl EventRouter {
    pub fn new(cfg: RouterConfig) -> Self {
        Self {
            field: cfg.field,
            rules: cfg.routes,
        }
    }

    pub fn route(&self, event: &Event) -> Vec<String> {
        let mut result = Vec::new();
        let v_opt = event.get_str(&self.field);
        for rule in &self.rules {
            if let Some(v) = v_opt {
                if v == rule.equals {
                    result.push(rule.sink.clone());
                }
            }
        }
        result
    }
}

/// =========================
///  Config structs
/// =========================

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub kind: String,

    // HTTP source config:
    #[serde(default)]
    pub address: Option<String>, // "0.0.0.0:9000"
    #[serde(default)]
    pub path: Option<String>,    // "/ingest"
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "type")]
    pub kind: String,

    // cho add_field
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub value: Option<String>,

    // cho contains_filter
    #[serde(default)]
    pub needle: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    #[serde(rename = "type")]
    pub kind: String,
    // HTTP sink config:
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct FullConfig {
    pub sources: HashMap<String, SourceConfig>,
    pub transforms: Option<HashMap<String, TransformConfig>>,
    pub router: Option<RouterConfig>,
    pub sinks: HashMap<String, SinkConfig>,
}

/// =========================
///  Registry & builders
/// =========================

fn build_source(name: &str, cfg: &SourceConfig) -> anyhow::Result<Box<dyn Source>> {
    match cfg.kind.as_str() {
        "stdin" => Ok(Box::new(StdinSource)),
        "http" => {
            let addr_str = cfg
                .address
                .clone()
                .ok_or_else(|| anyhow::anyhow!("source '{}' (http) missing 'address'", name))?;

            let path = cfg
                .path
                .clone()
                .unwrap_or_else(|| "/ingest".to_string());

            let addr: SocketAddr = addr_str
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid address '{}' for source '{}': {}", addr_str, name, e))?;

            Ok(Box::new(HttpSource::new(addr, path)))
        }
        other => anyhow::bail!("Unknown source type '{}' for '{}'", other, name),
    }
}

fn build_transform(name: &str, cfg: &TransformConfig) -> anyhow::Result<Box<dyn Transform>> {
    match cfg.kind.as_str() {
        "add_field" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'field'", name))?;
            let value = cfg
                .value
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'value'", name))?;
            Ok(Box::new(AddFieldTransform::new(field, Value::from(value))))
        }
        "contains_filter" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'field'", name))?;
            let needle = cfg
                .needle
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'needle'", name))?;
            Ok(Box::new(ContainsFilterTransform::new(field, needle)))
        }
        "json_parse" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (json_parse) missing 'field'", name))?;
            Ok(Box::new(JsonParseTransform::new(field)))
        }
        other => anyhow::bail!("Unknown transform type '{}' for '{}'", other, name),
    }
}


fn build_sink(name: &str, cfg: &SinkConfig) -> anyhow::Result<Box<dyn Sink>> {
    match cfg.kind.as_str() {
        "console" => Ok(Box::new(ConsoleSink::new(name.to_string()))),
        "http" => {
            let endpoint = cfg
                .endpoint
                .clone()
                .ok_or_else(|| anyhow::anyhow!("sink '{}' (http) missing 'endpoint'", name))?;
            Ok(Box::new(HttpSink::new(name.to_string(), endpoint)))
        }
        other => anyhow::bail!("Unknown sink type '{}' for '{}'", other, name),
    }
}

/// =========================
///  Pipeline runner
/// =========================

async fn run_pipeline(config: FullConfig) -> anyhow::Result<()> {
    // 1. Channel chung từ tất cả sources vào pipeline
    let (ingress_tx, mut ingress_rx) = mpsc::channel::<Event>(1024);

    // 2. Build sources
    let mut source_tasks = Vec::new();
    for (name, scfg) in &config.sources {
        let src = build_source(name, scfg)?;
        let tx = ingress_tx.clone();
        source_tasks.push(task::spawn(async move {
            src.run(tx).await;
        }));
    }
    drop(ingress_tx); // channel sẽ đóng khi các source kết thúc

    // 3. Build transforms (theo thứ tự định nghĩa trong file – hoặc anh có thể thêm trường "order")
    let mut transforms: Vec<Arc<dyn Transform>> = Vec::new();
    if let Some(tcfgs) = &config.transforms {
        for (name, tcfg) in tcfgs {
            let t = build_transform(name, tcfg)?;
            transforms.push(Arc::from(t));
        }
    }

    // 4. Build router
    let router = config
        .router
        .map(EventRouter::new)
        .unwrap_or_else(|| EventRouter::new(RouterConfig {
            field: "log_type".to_string(),
            routes: Vec::new(),
        }));

    // 5. Build sinks + channel cho từng sink
    let mut sink_senders: HashMap<String, mpsc::Sender<Event>> = HashMap::new();
    let mut sink_tasks = Vec::new();

    for (name, scfg) in &config.sinks {
        let sink = build_sink(name, scfg)?;
        let (tx, rx) = mpsc::channel::<Event>(1024);
        sink_senders.insert(name.clone(), tx);
        sink_tasks.push(task::spawn(async move {
            sink.run(rx).await;
        }));
    }

    let router = Arc::new(router);
    let sink_senders = Arc::new(sink_senders);
    let transforms = Arc::new(transforms);

    // 6. Task chính của pipeline: transform + route
    let pipeline_task = {
        let router = router.clone();
        let sink_senders = sink_senders.clone();
        let transforms = transforms.clone();

        task::spawn(async move {
            while let Some(mut event) = ingress_rx.recv().await {
                // áp dụng tất cả transforms
                let mut keep = true;
                for t in transforms.iter() {
                    if !t.apply(&mut event) {
                        keep = false;
                        break;
                    }
                }
                if !keep {
                    continue;
                }

                // route đến các sink
                let targets = router.route(&event);
                if targets.is_empty() {
                    // có thể log warning hoặc bỏ
                    continue;
                }

                for sink_id in targets {
                    if let Some(tx) = sink_senders.get(&sink_id) {
                        // clone event vì có thể gửi nhiều sink
                        if tx.send(event.clone()).await.is_err() {
                            warn!("Sink '{}' seems closed", sink_id);
                        }
                    } else {
                        warn!("No sink named '{}' in routing", sink_id);
                    }
                }
            }
            info!("Pipeline main loop exiting (no more events)");
        })
    };

    // 7. Chờ các task
    for t in source_tasks {
        let _ = t.await;
    }
    let _ = pipeline_task.await;
    for t in sink_tasks {
        let _ = t.await;
    }

    Ok(())
}

/// =========================
///  Main + load config
/// =========================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("mini_vector.yml"));

    let file = File::open(&config_path)?;
    let config: FullConfig = serde_yaml::from_reader(file)?;

    info!("Loaded config from {:?}", config_path);

    run_pipeline(config).await
}
