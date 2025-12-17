use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// Core value type carried inside events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Timestamp(DateTime<Utc>),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
    Bytes(Vec<u8>),
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Value::Bytes(value.to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEvent {
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Event {
    Log(LogEvent),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventMetadata {
    pub source: Option<String>,
    pub attempt: u32,
    pub received_at: Option<DateTime<Utc>>,
}

/// Event + metadata + ack handle that travels through the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event: Event,
    #[serde(default)]
    pub metadata: EventMetadata,
    #[serde(skip)]
    pub ack: AckToken,
}

impl EventEnvelope {
    pub fn new(event: Event) -> Self {
        Self {
            event,
            metadata: EventMetadata {
                attempt: 1,
                ..Default::default()
            },
            ack: AckToken::new(),
        }
    }

    pub fn with_source(event: Event, source: impl Into<String>) -> Self {
        Self {
            metadata: EventMetadata {
                source: Some(source.into()),
                attempt: 1,
                received_at: Some(Utc::now()),
            },
            ..Self::new(event)
        }
    }
}

pub struct AckToken {
    state: Arc<AckState>,
}

struct AckState {
    remaining: AtomicUsize,
    completed: AtomicBool,
    callbacks: Mutex<Vec<Box<dyn Fn() + Send + Sync>>>,
    notify: Notify,
}

impl AckState {
    fn new() -> Self {
        Self {
            remaining: AtomicUsize::new(1),
            completed: AtomicBool::new(false),
            callbacks: Mutex::new(Vec::new()),
            notify: Notify::new(),
        }
    }

    fn complete(&self) {
        if self
            .completed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let callbacks = std::mem::take(&mut *self.callbacks.lock().unwrap());
            for cb in callbacks {
                cb();
            }
            self.notify.notify_waiters();
        }
    }
}

impl AckToken {
    pub fn new() -> Self {
        Self {
            state: Arc::new(AckState::new()),
        }
    }

    /// Attach a callback that will be invoked exactly once when all clones are acked.
    pub fn on_complete<F>(&self, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        // If already completed, run immediately.
        if self.state.completed.load(Ordering::SeqCst) {
            f();
            return;
        }
        self.state.callbacks.lock().unwrap().push(Box::new(f));
    }

    /// Mark this token as acknowledged. The final ack (after all clones) will
    /// trigger callbacks and wake any waiters.
    pub fn ack(&self) {
        if self.state.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.state.complete();
        }
    }

    #[allow(dead_code)]
    pub async fn wait(&self) {
        if self.state.completed.load(Ordering::SeqCst) {
            return;
        }
        self.state.notify.notified().await;
    }
}

impl Default for AckToken {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for AckToken {
    fn clone(&self) -> Self {
        self.state.remaining.fetch_add(1, Ordering::SeqCst);
        Self {
            state: self.state.clone(),
        }
    }
}

impl fmt::Debug for AckToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AckToken")
    }
}

impl Event {
    pub fn new() -> Self {
        Event::Log(LogEvent {
            fields: BTreeMap::new(),
        })
    }

    pub fn insert<K: Into<String>, V: Into<Value>>(&mut self, key: K, value: V) {
        let Event::Log(log) = self;
        log.fields.insert(key.into(), value.into());
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self {
            Event::Log(log) => match log.fields.get(key) {
                Some(Value::String(s)) => Some(s.as_str()),
                _ => None,
            },
        }
    }

    pub fn as_log(&self) -> Option<&LogEvent> {
        Some(match self {
            Event::Log(log) => log,
        })
    }

    pub fn as_log_mut(&mut self) -> Option<&mut LogEvent> {
        Some(match self {
            Event::Log(log) => log,
        })
    }
}

/// Convert serde_json::Value -> internal Value recursively.
pub fn value_from_json(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(value_from_json).collect()),
        serde_json::Value::Object(map) => {
            let mut obj = BTreeMap::new();
            for (k, v2) in map {
                obj.insert(k.clone(), value_from_json(v2));
            }
            Value::Object(obj)
        }
    }
}

/// Try to parse a timestamp string into UTC DateTime.
pub fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Common alternative formats
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(Utc.from_utc_datetime(&dt));
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S") {
        return Some(Utc.from_utc_datetime(&dt));
    }

    // Epoch seconds
    if let Ok(secs) = s.parse::<i64>()
        && let chrono::LocalResult::Single(dt) = Utc.timestamp_opt(secs, 0)
    {
        return Some(dt);
    }

    None
}
