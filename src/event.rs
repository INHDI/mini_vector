use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, Utc};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Timestamp(DateTime<Utc>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Bytes(Vec<u8>),
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

    #[allow(dead_code)]
    pub fn get_timestamp(&self, key: &str) -> Option<DateTime<Utc>> {
        match self.fields.get(key) {
            Some(Value::Timestamp(ts)) => Some(*ts),
            Some(Value::String(s)) => parse_timestamp(s),
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
impl From<DateTime<Utc>> for Value {
    fn from(v: DateTime<Utc>) -> Self {
        Value::Timestamp(v)
    }
}

pub fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    // Try RFC3339 with offset then fallback to common layouts
    if let Ok(ts) = DateTime::parse_from_rfc3339(value) {
        return Some(ts.with_timezone(&Utc));
    }
    if let Ok(ts) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f %z") {
        return Some(ts.with_timezone(&Utc));
    }
    if let Ok(ts) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc));
    }
    None
}

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
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(value_from_json).collect())
        }
        serde_json::Value::Object(map) => {
            let mut out = HashMap::new();
            for (k, v) in map {
                out.insert(k.clone(), value_from_json(v));
            }
            Value::Object(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rfc3339_timestamp() {
        let s = "2024-01-02T03:04:05Z";
        let ts = parse_timestamp(s).expect("should parse rfc3339");
        assert_eq!(ts, DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc));
    }

    #[test]
    fn json_value_to_enum_variants() {
        let json = serde_json::json!({
            "null": null,
            "bool": true,
            "int": 42,
            "float": 1.5,
            "str": "hi",
            "arr": [1, 2, null],
            "obj": { "k": "v" }
        });

        if let Value::Object(map) = value_from_json(&json) {
            assert!(matches!(map["null"], Value::Null));
            assert!(matches!(map["bool"], Value::Bool(true)));
            assert!(matches!(map["int"], Value::Integer(42)));
            assert!(matches!(map["float"], Value::Float(f) if (f - 1.5).abs() < 1e-6));
            assert!(matches!(map["str"], Value::String(ref s) if s == "hi"));
            assert!(matches!(map["arr"], Value::Array(ref arr) if arr.len() == 3));
            assert!(matches!(map["obj"], Value::Object(ref o) if o["k"] == Value::String("v".to_string())));
        } else {
            panic!("expected object conversion");
        }
    }
}
