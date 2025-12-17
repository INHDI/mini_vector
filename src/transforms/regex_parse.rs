use crate::event::{Event, EventEnvelope};
use crate::transforms::Transform;
use async_trait::async_trait;
use regex::Regex;
use tokio::sync::mpsc;
use tracing::warn;

pub struct RegexParseTransform {
    pub name: String,
    pub field: String,
    pub regex: Regex,
    pub drop_on_error: bool,
    pub remove_source: bool,
    pub target_prefix: Option<String>,
}

impl RegexParseTransform {
    pub fn new(
        name: String,
        field: String,
        pattern: String,
        drop_on_error: bool,
        remove_source: bool,
        target_prefix: Option<String>,
    ) -> anyhow::Result<Self> {
        let regex = Regex::new(&pattern)
            .map_err(|e| anyhow::anyhow!("invalid regex pattern '{}': {}", pattern, e))?;
        Ok(Self {
            name,
            field,
            regex,
            drop_on_error,
            remove_source,
            target_prefix,
        })
    }
}

#[async_trait]
impl Transform for RegexParseTransform {
    async fn run(
        self: Box<Self>,
        mut input: mpsc::Receiver<EventEnvelope>,
        output: mpsc::Sender<EventEnvelope>,
    ) {
        while let Some(mut event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());

            let raw = match event.event.get_str(&self.field) {
                Some(s) => s.to_string(),
                None => {
                    if self.drop_on_error {
                        warn!("RegexParseTransform: source field '{}' missing", self.field);
                        metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "missing_field");
                        event.ack.ack();
                        continue;
                    }
                    match output.send(event).await {
                        Ok(_) => {
                            metrics::increment_counter!("events_out", "component" => self.name.clone())
                        }
                        Err(err) => {
                            let ev = err.0;
                            ev.ack.ack();
                            break;
                        }
                    }
                    continue;
                }
            };

            if raw.trim().is_empty() {
                if self.drop_on_error {
                    metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "empty_field");
                    event.ack.ack();
                    continue;
                }
                match output.send(event).await {
                    Ok(_) => {
                        metrics::increment_counter!("events_out", "component" => self.name.clone())
                    }
                    Err(err) => {
                        let ev = err.0;
                        ev.ack.ack();
                        break;
                    }
                }
                continue;
            }

            if let Some(caps) = self.regex.captures(&raw) {
                for name in self.regex.capture_names().flatten().filter(|n| *n != "0") {
                    if let Some(m) = caps.name(name) {
                        let key = if let Some(prefix) = &self.target_prefix {
                            format!("{}.{}", prefix, name)
                        } else {
                            name.to_string()
                        };
                        event.event.insert(key, m.as_str().to_string());
                    }
                }

                if self.remove_source {
                    let Event::Log(log) = &mut event.event;
                    log.fields.remove(&self.field);
                }

                match output.send(event).await {
                    Ok(_) => {
                        metrics::increment_counter!("events_out", "component" => self.name.clone())
                    }
                    Err(err) => {
                        let ev = err.0;
                        ev.ack.ack();
                        break;
                    }
                }
            } else {
                warn!(
                    "RegexParseTransform: regex did not match field '{}': {}",
                    self.field, raw
                );
                if self.drop_on_error {
                    metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "no_match");
                    event.ack.ack();
                } else {
                    event.event.insert("regex_parse_error".to_string(), true);
                    match output.send(event).await {
                        Ok(_) => {
                            metrics::increment_counter!("events_out", "component" => self.name.clone())
                        }
                        Err(err) => {
                            let ev = err.0;
                            ev.ack.ack();
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Value;

    #[tokio::test]
    async fn parses_named_groups_and_removes_source() {
        let mut event = Event::new();
        event.insert("message", "prog: hello");
        let envelope = EventEnvelope::new(event);
        let t = RegexParseTransform::new(
            "test".into(),
            "message".into(),
            r"(?P<program>\w+): (?P<msg>.+)".into(),
            true,
            true,
            None,
        )
        .unwrap();

        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(envelope).await.unwrap();
        drop(tx_in);

        Box::new(t).run(rx_in, tx_out).await;

        let event = rx_out.recv().await.expect("should output event");
        let log = event.event.as_log().unwrap();

        assert!(!log.fields.contains_key("message"));
        assert_eq!(
            log.fields.get("program"),
            Some(&Value::String("prog".to_string()))
        );
        assert_eq!(
            log.fields.get("msg"),
            Some(&Value::String("hello".to_string()))
        );
    }

    #[tokio::test]
    async fn uses_target_prefix_and_sets_error_flag_on_non_drop() {
        let mut event = Event::new();
        event.insert("message", "no match");
        let envelope = EventEnvelope::new(event);
        let t = RegexParseTransform::new(
            "test".into(),
            "message".into(),
            r"(?P<program>\w+): (?P<msg>.+)".into(),
            false,
            false,
            Some("parsed".to_string()),
        )
        .unwrap();

        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(envelope).await.unwrap();
        drop(tx_in);

        Box::new(t).run(rx_in, tx_out).await;

        let event = rx_out.recv().await.expect("should output event");
        let log = event.event.as_log().unwrap();

        assert_eq!(
            log.fields.get("regex_parse_error"),
            Some(&Value::Bool(true))
        );
        assert!(!log.fields.contains_key("parsed.program"));
    }
}
