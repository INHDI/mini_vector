use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::warn;
use crate::event::{value_from_json, Event};
use crate::transforms::Transform;

pub struct JsonParseTransform {
    pub name: String,
    pub from_field: String,
    pub drop_on_error: bool,
    pub remove_source: bool,
    pub target_prefix: Option<String>,
}

impl JsonParseTransform {
    pub fn new(
        name: String,
        from_field: String,
        drop_on_error: bool,
        remove_source: bool,
        target_prefix: Option<String>,
    ) -> Self {
        Self {
            name,
            from_field,
            drop_on_error,
            remove_source,
            target_prefix,
        }
    }
}

#[async_trait]
impl Transform for JsonParseTransform {
    async fn run(self: Box<Self>, mut input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>) {
        while let Some(mut event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());

            let raw = match event.get_str(&self.from_field) {
                Some(s) => s.to_string(),
                None => {
                    warn!("JsonParseTransform: source field '{}' missing or not string", self.from_field);
                    if self.drop_on_error {
                        metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "missing_field");
                        continue;
                    } else {
                        event.insert("json_parse_error".to_string(), "missing_source_field");
                        if output.send(event).await.is_err() { break; }
                        metrics::increment_counter!("events_out", "component" => self.name.clone());
                        continue;
                    }
                }
            };

            if raw.trim().is_empty() {
                 if self.drop_on_error {
                     metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "empty_field");
                     continue;
                 }
                 // pass through
                 if output.send(event).await.is_err() { break; }
                 metrics::increment_counter!("events_out", "component" => self.name.clone());
                 continue;
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
                         let Event::Log(log) = &mut event;
                         log.fields.remove(&self.from_field);
                    }
                    
                    if output.send(event).await.is_err() { break; }
                    metrics::increment_counter!("events_out", "component" => self.name.clone());
                }
                Err(e) => {
                     warn!("JsonParseTransform: failed to parse '{}' as JSON: {}", self.from_field, e);
                     if self.drop_on_error {
                         metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "parse_error");
                     } else {
                         event.insert("json_parse_error".to_string(), e.to_string());
                         if output.send(event).await.is_err() { break; }
                         metrics::increment_counter!("events_out", "component" => self.name.clone());
                     }
                }
                _ => {
                    if self.drop_on_error {
                         metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "not_object");
                    } else {
                         event.insert("json_parse_error".to_string(), "not_a_json_object");
                         if output.send(event).await.is_err() { break; }
                         metrics::increment_counter!("events_out", "component" => self.name.clone());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use crate::event::Value;

    #[tokio::test]
    async fn parses_json_object_into_fields() {
        let mut event = Event::new();
        event.insert("message", r#"{"message":"hello","x":1}"#.to_string());
        
        let t = JsonParseTransform::new("test".into(), "message".into(), false, true, None);
        
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);
        
        Box::new(t).run(rx_in, tx_out).await;
        
        let event = rx_out.recv().await.expect("should output event");
        let log = event.as_log().unwrap();
        
        assert!(!log.fields.contains_key("message"));
        assert_eq!(
            log.fields.get("x"),
            Some(&Value::Integer(1))
        );
    }

    #[tokio::test]
    async fn converts_nested_structures() {
        let mut event = Event::new();
        let payload = json!({
            "a": { "b": [1, 2, null] },
            "t": "2024-01-02T03:04:05Z"
        })
        .to_string();
        event.insert("message", payload);
        
        let t = JsonParseTransform::new("test".into(), "message".into(), false, false, Some("parsed".to_string()));
        
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);
        
        Box::new(t).run(rx_in, tx_out).await;
        
        let event = rx_out.recv().await.expect("should output event");
        let log = event.as_log().unwrap();

        match log.fields.get("parsed.a") {
            Some(Value::Object(map)) => match map.get("b") {
                Some(Value::Array(arr)) => {
                    assert_eq!(arr.len(), 3);
                }
                _ => panic!("expected array"),
            },
            _ => panic!("expected object"),
        }
        match log.fields.get("parsed.t") {
            Some(Value::String(s)) => assert_eq!(s, "2024-01-02T03:04:05Z"),
            _ => panic!("expected string"),
        }
        assert!(log.fields.get("a").is_none());
    }

    #[tokio::test]
    async fn drops_on_error_when_configured() {
        let mut event = Event::new();
        event.insert("message", "{invalid json".to_string());
        
        let t = JsonParseTransform::new("test".into(), "message".into(), true, false, None);
        
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);
        
        Box::new(t).run(rx_in, tx_out).await;
        
        assert!(rx_out.recv().await.is_none());
    }
}
