use crate::event::{value_from_json, Event, Value};
use crate::transforms::Transform;
use tracing::warn;

pub struct JsonParseTransform {
    pub from_field: String,
    pub drop_on_error: bool,
    pub remove_source: bool,
    pub target_prefix: Option<String>,
}

impl JsonParseTransform {
    pub fn new(
        from_field: String,
        drop_on_error: bool,
        remove_source: bool,
        target_prefix: Option<String>,
    ) -> Self {
        Self {
            from_field,
            drop_on_error,
            remove_source,
            target_prefix,
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
                    let key = if let Some(prefix) = &self.target_prefix {
                        format!("{}.{}", prefix, k)
                    } else {
                        k
                    };
                    event.insert(key, value_from_json(&v));
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_json_object_into_fields() {
        let mut event = Event::new();
        event.insert("message", r#"{"message":"hello","x":1}"#.to_string());
        let t = JsonParseTransform::new("message".into(), false, true, None);
        let keep = t.apply(&mut event);
        assert!(keep);
        assert!(!event.fields.contains_key("message"));
        assert_eq!(
            event.fields.get("x"),
            Some(&Value::Integer(1))
        );
    }

    #[test]
    fn converts_nested_structures() {
        let mut event = Event::new();
        let payload = json!({
            "a": { "b": [1, 2, null] },
            "t": "2024-01-02T03:04:05Z"
        })
        .to_string();
        event.insert("message", payload);
        let t = JsonParseTransform::new("message".into(), false, false, Some("parsed".to_string()));
        let keep = t.apply(&mut event);
        assert!(keep);
        match event.fields.get("parsed.a") {
            Some(Value::Object(map)) => match map.get("b") {
                Some(Value::Array(arr)) => {
                    assert_eq!(arr.len(), 3);
                }
                _ => panic!("expected array"),
            },
            _ => panic!("expected object"),
        }
        match event.fields.get("parsed.t") {
            Some(Value::String(s)) => assert_eq!(s, "2024-01-02T03:04:05Z"),
            _ => panic!("expected string"),
        }
        assert!(event.fields.get("a").is_none());
    }

    #[test]
    fn drops_on_error_when_configured() {
        let mut event = Event::new();
        event.insert("message", "{invalid json".to_string());
        let t = JsonParseTransform::new("message".into(), true, false, None);
        let keep = t.apply(&mut event);
        assert!(!keep);
    }
}
