use std::collections::BTreeMap;

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
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(value_from_json).collect())
        }
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
    if let Ok(secs) = s.parse::<i64>() {
        if let chrono::LocalResult::Single(dt) = Utc.timestamp_opt(secs, 0) {
            return Some(dt);
        }
    }

    None
}
