use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{info, warn};

/// =========================
///  Event model
/// =========================

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

/// =========================
///  Implementations: Sinks
/// =========================

pub struct ConsoleSink {
    name: String,
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

pub struct Router {
    field: String,
    rules: Vec<RouteRuleConfig>,
}

impl Router {
    pub fn new(cfg: RouterConfig) -> Self {
        Self {
            field: cfg.field,
            rules: cfg.routes,
        }
    }

    /// Trả về danh sách sink_id phù hợp với event.
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
        // Nếu không match rule nào, có thể trả về sink default (tuỳ anh thiết kế)
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
        other => anyhow::bail!("Unknown transform type '{}' for '{}'", other, name),
    }
}

fn build_sink(name: &str, cfg: &SinkConfig) -> anyhow::Result<Box<dyn Sink>> {
    match cfg.kind.as_str() {
        "console" => Ok(Box::new(ConsoleSink::new(name.to_string()))),
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
        .map(Router::new)
        .unwrap_or_else(|| Router::new(RouterConfig {
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
