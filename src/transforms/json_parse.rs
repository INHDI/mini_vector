use crate::event::{Event, Value};
use crate::transforms::Transform;
use tracing::warn;

pub struct JsonParseTransform {
    pub from_field: String,
    pub drop_on_error: bool,
    pub remove_source: bool,
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
