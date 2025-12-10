use chrono::Utc;

use crate::event::{parse_timestamp, Event, Value};
use crate::transforms::Transform;

/// NormalizeSchemaTransform: map field nguồn về schema Mini SOC
/// Canonical keys: @timestamp, host, severity, program, message, log_type
pub struct NormalizeSchemaTransform {
    pub timestamp_field: Option<String>,
    pub host_field: Option<String>,
    pub severity_field: Option<String>,
    pub program_field: Option<String>,
    pub message_field: Option<String>,
    pub default_log_type: Option<String>,
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

fn normalize_severity(value: Option<&Value>) -> Value {
    let as_string = match value {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Integer(i)) => Some(i.to_string()),
        Some(Value::Float(f)) => Some(f.trunc().to_string()),
        _ => None,
    };

    if let Some(s) = as_string {
        let lower = s.to_ascii_lowercase();
        let normalized = if let Ok(num) = lower.parse::<u8>() {
            match num {
                0..=3 => "ERROR",
                4 => "WARN",
                5..=6 => "INFO",
                _ => "DEBUG",
            }
        } else if ["emerg", "fatal", "alert", "crit", "critical", "err", "error"]
            .contains(&lower.as_str())
        {
            "ERROR"
        } else if ["warn", "warning", "notice"].contains(&lower.as_str()) {
            "WARN"
        } else if ["debug", "trace"].contains(&lower.as_str()) {
            "DEBUG"
        } else {
            "INFO"
        };
        return Value::String(normalized.to_string());
    }

    Value::String("INFO".to_string())
}

impl Transform for NormalizeSchemaTransform {
    fn apply(&self, event: &mut Event) -> bool {
        // timestamp
        let ts_candidates = [
            self.timestamp_field.as_deref(),
            Some("@timestamp"),
            Some("timestamp"),
            Some("time"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let ts_value = ts_candidates
            .iter()
            .find_map(|src| {
                event.fields.get(*src).and_then(|v| match v {
                    Value::Timestamp(t) => Some(Value::Timestamp(*t)),
                    Value::String(s) => parse_timestamp(s).map(Value::Timestamp),
                    _ => None,
                })
            })
            .unwrap_or_else(|| Value::Timestamp(Utc::now()));
        let ts_value = ts_value
            .clone();
        event.insert("@timestamp".to_string(), ts_value);

        // host/program/message passthrough with defaults
        let host_candidates = [
            self.host_field.as_deref(),
            Some("hostname"),
            Some("host"),
        ];
        for src in host_candidates.into_iter().flatten() {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("host".to_string(), v);
                break;
            }
        }
        if !event.fields.contains_key("host") {
            event.insert("host".to_string(), Value::String("unknown".to_string()));
        }

        let program_candidates = [
            self.program_field.as_deref(),
            Some("program"),
            Some("app"),
            Some("service"),
        ];
        for src in program_candidates.into_iter().flatten() {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("program".to_string(), v);
                break;
            }
        }
        if !event.fields.contains_key("program") {
            event.insert("program".to_string(), Value::String("unknown".to_string()));
        }

        let message_candidates = [
            self.message_field.as_deref(),
            Some("msg"),
            Some("message"),
        ];
        for src in message_candidates.into_iter().flatten() {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("message".to_string(), v);
                break;
            }
        }
        if !event.fields.contains_key("message") {
            event.insert("message".to_string(), Value::String(String::new()));
        }

        let severity_candidates = [
            self.severity_field.as_deref(),
            Some("severity"),
            Some("level"),
        ];
        let mut severity_set = false;
        for src in severity_candidates.into_iter().flatten() {
            if event.fields.contains_key(src) {
                let sev_val = normalize_severity(event.fields.get(src));
                event.insert("severity".to_string(), sev_val);
                severity_set = true;
                break;
            }
        }
        if !severity_set {
            event.insert("severity".to_string(), Value::String("INFO".to_string()));
        }

        if !event.fields.contains_key("log_type") {
            let lt = self
                .default_log_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            event.insert("log_type".to_string(), Value::String(lt));
        }

        if !event.fields.contains_key("soc_tenant") {
            event.insert("soc_tenant".to_string(), Value::String("unknown".to_string()));
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;

    #[test]
    fn sets_defaults_when_missing() {
        let mut event = Event::new();
        let t0 = Utc::now();
        let t = NormalizeSchemaTransform::new(None, None, None, None, None, None);
        let keep = t.apply(&mut event);
        assert!(keep);

        match event.fields.get("@timestamp") {
            Some(Value::Timestamp(ts)) => {
                assert!((*ts - t0) < Duration::seconds(5));
            }
            other => panic!("expected timestamp, got {:?}", other),
        }
        assert_eq!(
            event.fields.get("log_type"),
            Some(&Value::String("unknown".to_string()))
        );
        assert_eq!(
            event.fields.get("severity"),
            Some(&Value::String("INFO".to_string()))
        );
        assert_eq!(
            event.fields.get("soc_tenant"),
            Some(&Value::String("unknown".to_string()))
        );
    }

    #[test]
    fn parses_timestamp_from_string_field_and_uses_default_log_type() {
        let mut event = Event::new();
        event.insert("ts_raw", "2024-01-02T03:04:05Z");
        let t = NormalizeSchemaTransform::new(
            Some("ts_raw".to_string()),
            None,
            None,
            None,
            Some("message".to_string()),
            Some("custom".to_string()),
        );
        let keep = t.apply(&mut event);
        assert!(keep);
        match event.fields.get("@timestamp") {
            Some(Value::Timestamp(ts)) => {
                assert_eq!(ts.to_rfc3339(), "2024-01-02T03:04:05+00:00");
            }
            other => panic!("expected timestamp, got {:?}", other),
        }
        assert_eq!(
            event.fields.get("log_type"),
            Some(&Value::String("custom".to_string()))
        );
    }

    #[test]
    fn normalizes_severity_from_string_and_number() {
        let mut event = Event::new();
        event.insert("sev", "warning");
        event.insert("sev_num", 2_i64);
        let t = NormalizeSchemaTransform::new(None, None, Some("sev".to_string()), None, None, None);
        t.apply(&mut event);
        assert_eq!(
            event.fields.get("severity"),
            Some(&Value::String("WARN".to_string()))
        );

        let t2 = NormalizeSchemaTransform::new(
            None,
            None,
            Some("sev_num".to_string()),
            None,
            None,
            None,
        );
        t2.apply(&mut event);
        assert_eq!(
            event.fields.get("severity"),
            Some(&Value::String("ERROR".to_string()))
        );
    }
}
