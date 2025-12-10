use std::collections::HashMap;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{extract::State, routing::post, Json, Router as HttpRouter};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{info, warn};
use regex::Regex;

/// =========================
///  HTTP Source
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

async fn http_source_handler(State(state): State<HttpSourceState>, Json(body): Json<JsonValue>) {
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
                return;
            }
        };

        if let Err(err) = axum::serve(listener, app).await {
            warn!("HttpSource error: {}", err);
        }

        info!("HttpSource exiting");
    }
}

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

/// Filter theo substring
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

/// JsonParseTransform với behavior chuẩn hóa:
/// - drop_on_error: true => drop event khi parse lỗi
/// - remove_source: true => xóa field nguồn (ví dụ message) sau khi parse thành công
pub struct JsonParseTransform {
    from_field: String,
    drop_on_error: bool,
    remove_source: bool,
}

impl JsonParseTransform {
    pub fn new(from_field: String, drop_on_error: bool, remove_source: bool) -> Self {
        Self {
            from_field,
            drop_on_error,
            remove_source,
        }
    }
}

impl Transform for JsonParseTransform {
    fn apply(&self, event: &mut Event) -> bool {
        let raw = match event.fields.get(&self.from_field) {
            Some(Value::String(s)) => s.clone(),
            _ => {
                // không có field hoặc không phải string
                if self.drop_on_error {
                    warn!(
                        "JsonParseTransform: source field '{}' missing or not string, dropping event",
                        self.from_field
                    );
                    return false;
                } else {
                    return true;
                }
            }
        };

        if raw.trim().is_empty() {
            // chuỗi rỗng: coi như không có dữ liệu
            return !self.drop_on_error;
        }

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
                if self.remove_source {
                    event.fields.remove(&self.from_field);
                }
                true
            }
            _ => {
                warn!(
                    "JsonParseTransform: failed to parse '{}' as JSON",
                    self.from_field
                );
                if self.drop_on_error {
                    false
                } else {
                    true
                }
            }
        }
    }
}

/// RegexParseTransform:
/// - field: tên field nguồn (ví dụ: "message")
/// - pattern: regex với named capture groups (?P<name>...)
/// - drop_on_error: true => drop event khi không match
/// - remove_source: true => xóa field nguồn sau khi parse thành công
pub struct RegexParseTransform {
    field: String,
    regex: Regex,
    drop_on_error: bool,
    remove_source: bool,
}

impl RegexParseTransform {
    pub fn new(
        field: String,
        pattern: String,
        drop_on_error: bool,
        remove_source: bool,
    ) -> anyhow::Result<Self> {
        let regex = Regex::new(&pattern)
            .map_err(|e| anyhow::anyhow!("invalid regex pattern '{}': {}", pattern, e))?;
        Ok(Self {
            field,
            regex,
            drop_on_error,
            remove_source,
        })
    }
}

impl Transform for RegexParseTransform {
    fn apply(&self, event: &mut Event) -> bool {
        let raw = match event.fields.get(&self.field) {
            Some(Value::String(s)) => s.clone(),
            _ => {
                if self.drop_on_error {
                    warn!(
                        "RegexParseTransform: source field '{}' missing or not string, dropping event",
                        self.field
                    );
                    return false;
                } else {
                    return true;
                }
            }
        };

        if raw.trim().is_empty() {
            return !self.drop_on_error;
        }

        if let Some(caps) = self.regex.captures(&raw) {
            // Lặp qua tất cả named groups (bỏ group 0)
            for name in self
                .regex
                .capture_names()
                .flatten()
                .filter(|n| *n != "0")
            {
                if let Some(m) = caps.name(name) {
                    event.insert(name.to_string(), m.as_str().to_string());
                }
            }

            if self.remove_source {
                event.fields.remove(&self.field);
            }

            true
        } else {
            warn!(
                "RegexParseTransform: regex did not match field '{}' value: {}",
                self.field, raw
            );
            !self.drop_on_error
        }
    }
}

/// NormalizeSchemaTransform: map field nguồn về schema Mini SOC
/// Canonical keys: @timestamp, host, severity, program, message, log_type
pub struct NormalizeSchemaTransform {
    timestamp_field: Option<String>,
    host_field: Option<String>,
    severity_field: Option<String>,
    program_field: Option<String>,
    message_field: Option<String>,
    default_log_type: Option<String>,
}

impl NormalizeSchemaTransform {
    pub fn new(
        timestamp_field: Option<String>,
        host_field: Option<String>,
        severity_field: Option<String>,
        program_field: Option<String>,
        message_field: Option<String>,
        default_log_type: Option<String>,
    ) -> Self {
        Self {
            timestamp_field,
            host_field,
            severity_field,
            program_field,
            message_field,
            default_log_type,
        }
    }
}

impl Transform for NormalizeSchemaTransform {
    fn apply(&self, event: &mut Event) -> bool {
        if let Some(ref src) = self.timestamp_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("@timestamp".to_string(), v);
            }
        }
        if let Some(ref src) = self.host_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("host".to_string(), v);
            }
        }
        if let Some(ref src) = self.severity_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("severity".to_string(), v);
            }
        }
        if let Some(ref src) = self.program_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("program".to_string(), v);
            }
        }
        if let Some(ref src) = self.message_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("message".to_string(), v);
            }
        }

        // đảm bảo có log_type nếu default được cấu hình
        if !event.fields.contains_key("log_type") {
            if let Some(ref lt) = self.default_log_type {
                event.insert("log_type".to_string(), Value::String(lt.clone()));
            }
        }

        true
    }
}

/// ScriptTransform – "mini VRL" rất đơn giản:
/// Hỗ trợ các câu lệnh:
///   .field = "literal"
///   .field = .other_field
///   .field = upcase(.other_field)
pub struct ScriptTransform {
    lines: Vec<String>,
}

impl ScriptTransform {
    pub fn new(script: String) -> Self {
        let lines = script
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        Self { lines }
    }

    fn apply_line(&self, line: &str, event: &mut Event) {
        if !line.starts_with('.') {
            warn!(
                "ScriptTransform: invalid line (must start with '.'): {}",
                line
            );
            return;
        }
        let parts: Vec<&str> = line.splitn(2, '=').collect();
        if parts.len() != 2 {
            warn!("ScriptTransform: invalid assignment: {}", line);
            return;
        }
        let left = parts[0].trim();
        let right = parts[1].trim();

        if !left.starts_with('.') {
            warn!("ScriptTransform: invalid left side: {}", line);
            return;
        }
        let field_name = &left[1..];

        // .field = "literal"
        if right.starts_with('"') && right.ends_with('"') && right.len() >= 2 {
            let inner = &right[1..right.len() - 1];
            event.insert(field_name.to_string(), Value::String(inner.to_string()));
            return;
        }

        // .field = .other_field
        if right.starts_with('.') {
            let src = &right[1..];
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert(field_name.to_string(), v);
            } else {
                event.insert(field_name.to_string(), Value::String(String::new()));
            }
            return;
        }

        // .field = upcase(.other_field)
        if right.starts_with("upcase(") && right.ends_with(')') {
            let inner = &right["upcase(".len()..right.len() - 1];
            let inner = inner.trim();
            if inner.starts_with('.') {
                let src = &inner[1..];
                if let Some(Value::String(s)) = event.fields.get(src) {
                    event.insert(field_name.to_string(), Value::String(s.to_uppercase()));
                }
            }
            return;
        }

        warn!("ScriptTransform: unsupported expression: {}", line);
    }
}

impl Transform for ScriptTransform {
    fn apply(&self, event: &mut Event) -> bool {
        for line in &self.lines {
            self.apply_line(line, event);
        }
        true
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
            println!(
                "[sink:{}] {}",
                self.name,
                serde_json::to_string(&event).unwrap()
            );
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
    pub path: Option<String>, // "/ingest"
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "type")]
    pub kind: String,

    // add_field / contains_filter / json_parse
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub needle: Option<String>,

    // json_parse options
    #[serde(default)]
    pub drop_on_error: Option<bool>,
    #[serde(default)]
    pub remove_source: Option<bool>,

    // normalize_schema options
    #[serde(default)]
    pub timestamp_field: Option<String>,
    #[serde(default)]
    pub host_field: Option<String>,
    #[serde(default)]
    pub severity_field: Option<String>,
    #[serde(default)]
    pub program_field: Option<String>,
    #[serde(default)]
    pub message_field: Option<String>,
    #[serde(default)]
    pub default_log_type: Option<String>,

    // script transform
    #[serde(default)]
    pub script: Option<String>,

    #[serde(default)]
    pub pattern: Option<String>,
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

            let addr: SocketAddr = addr_str.parse().map_err(|e| {
                anyhow::anyhow!(
                    "invalid address '{}' for source '{}': {}",
                    addr_str,
                    name,
                    e
                )
            })?;

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
            Ok(Box::new(AddFieldTransform::new(
                field,
                Value::from(value),
            )))
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
            let drop_on_error = cfg.drop_on_error.unwrap_or(false);
            let remove_source = cfg.remove_source.unwrap_or(false);
            Ok(Box::new(JsonParseTransform::new(
                field,
                drop_on_error,
                remove_source,
            )))
        }
        "normalize_schema" => {
            Ok(Box::new(NormalizeSchemaTransform::new(
                cfg.timestamp_field.clone(),
                cfg.host_field.clone(),
                cfg.severity_field.clone(),
                cfg.program_field.clone(),
                cfg.message_field.clone(),
                cfg.default_log_type.clone(),
            )))
        }
        "script" => {
            let script = cfg
                .script
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (script) missing 'script'", name))?;
            Ok(Box::new(ScriptTransform::new(script)))
        }
        "regex_parse" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (regex_parse) missing 'field'", name))?;
            let pattern = cfg
                .pattern
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (regex_parse) missing 'pattern'", name))?;

            let drop_on_error = cfg.drop_on_error.unwrap_or(false);
            let remove_source = cfg.remove_source.unwrap_or(false);

            let t = RegexParseTransform::new(field, pattern, drop_on_error, remove_source)?;
            Ok(Box::new(t))
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
            Ok(Box::new(HttpSink::new(
                name.to_string(),
                endpoint,
            )))
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

    // 3. Build transforms
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
                    continue;
                }

                for sink_id in targets {
                    if let Some(tx) = sink_senders.get(&sink_id) {
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
