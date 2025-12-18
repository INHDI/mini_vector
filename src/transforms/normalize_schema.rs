use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::event::{Event, EventEnvelope, Value, parse_timestamp};
use crate::transforms::Transform;

/// NormalizeSchemaTransform: map field nguồn về schema Mini SOC
/// Canonical keys: @timestamp, host, severity, program, message, log_type
pub struct NormalizeSchemaTransform {
    pub name: String,
    pub timestamp_field: Option<String>,
    pub host_field: Option<String>,
    pub severity_field: Option<String>,
    pub program_field: Option<String>,
    pub message_field: Option<String>,
    pub default_log_type: Option<String>,
    pub default_tenant: Option<String>,
}

impl NormalizeSchemaTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        timestamp_field: Option<String>,
        host_field: Option<String>,
        severity_field: Option<String>,
        program_field: Option<String>,
        message_field: Option<String>,
        default_log_type: Option<String>,
        default_tenant: Option<String>,
    ) -> Self {
        Self {
            name,
            timestamp_field,
            host_field,
            severity_field,
            program_field,
            message_field,
            default_log_type,
            default_tenant,
        }
    }

    fn apply_logic(&self, event: &mut Event) {
        let Event::Log(log) = event;

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
                log.fields.get(*src).and_then(|v| match v {
                    Value::Timestamp(t) => Some(Value::Timestamp(*t)),
                    Value::String(s) => parse_timestamp(s).map(Value::Timestamp),
                    _ => None,
                })
            })
            .unwrap_or_else(|| Value::Timestamp(Utc::now()));
        log.fields.insert("@timestamp".to_string(), ts_value);

        // host/program/message passthrough with defaults
        let host_candidates = [self.host_field.as_deref(), Some("hostname"), Some("host")];
        for src in host_candidates.into_iter().flatten() {
            if let Some(v) = log.fields.get(src).cloned() {
                log.fields.insert("host".to_string(), v);
                break;
            }
        }
        if !log.fields.contains_key("host") {
            log.fields
                .insert("host".to_string(), Value::String("unknown".to_string()));
        }

        let program_candidates = [
            self.program_field.as_deref(),
            Some("program"),
            Some("app"),
            Some("service"),
        ];
        for src in program_candidates.into_iter().flatten() {
            if let Some(v) = log.fields.get(src).cloned() {
                log.fields.insert("program".to_string(), v);
                break;
            }
        }
        if !log.fields.contains_key("program") {
            log.fields
                .insert("program".to_string(), Value::String("unknown".to_string()));
        }

        let message_candidates = [self.message_field.as_deref(), Some("msg"), Some("message")];
        for src in message_candidates.into_iter().flatten() {
            if let Some(v) = log.fields.get(src).cloned() {
                log.fields.insert("message".to_string(), v);
                break;
            }
        }
        if !log.fields.contains_key("message") {
            log.fields
                .insert("message".to_string(), Value::String(String::new()));
        }

        let severity_candidates = [
            self.severity_field.as_deref(),
            Some("severity"),
            Some("level"),
        ];
        let mut severity_set = false;
        for src in severity_candidates.into_iter().flatten() {
            if log.fields.contains_key(src) {
                let sev_val = normalize_severity(log.fields.get(src));
                log.fields.insert("severity".to_string(), sev_val);
                severity_set = true;
                break;
            }
        }
        if !severity_set {
            log.fields
                .insert("severity".to_string(), Value::String("INFO".to_string()));
        }

        if !log.fields.contains_key("log_type") {
            let lt = self
                .default_log_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            log.fields.insert("log_type".to_string(), Value::String(lt));
        }

        if !log.fields.contains_key("soc_tenant") {
            let tenant = self
                .default_tenant
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            log.fields
                .insert("soc_tenant".to_string(), Value::String(tenant));
        }

        // raw_log: prefer explicit raw_log/message_raw; otherwise leave absent
        if !log.fields.contains_key("raw_log")
            && let Some(raw) = log
                .fields
                .get("raw_log")
                .cloned()
                .or_else(|| log.fields.get("message_raw").cloned())
        {
            log.fields.insert("raw_log".to_string(), raw);
        }

        // security/common mappings
        copy_if_present(log, "src_ip", &["src_ip", "client_ip", "source_ip"]);
        copy_if_present(log, "src_port", &["src_port", "source_port"]);
        copy_if_present(
            log,
            "dest_ip",
            &["dest_ip", "destination_ip", "dst_ip", "server_ip"],
        );
        copy_if_present(
            log,
            "dest_port",
            &["dest_port", "destination_port", "dst_port", "server_port"],
        );
        copy_if_present(log, "user", &["user", "username", "account"]);
        copy_if_present(log, "action", &["action", "verb", "method"]);
        copy_if_present(
            log,
            "result",
            &["result", "status", "status_code", "outcome"],
        );
        copy_if_present(log, "rule_id", &["rule_id", "signature_id", "sid"]);
        copy_if_present(log, "rule_name", &["rule_name", "signature", "policy"]);
    }
}

fn copy_if_present(log: &mut crate::event::LogEvent, target: &str, candidates: &[&str]) {
    for src in candidates {
        if let Some(v) = log.fields.get(*src).cloned() {
            log.fields.insert(target.to_string(), v);
            break;
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
        } else if [
            "emerg", "fatal", "alert", "crit", "critical", "err", "error",
        ]
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

#[async_trait]
impl Transform for NormalizeSchemaTransform {
    async fn run(
        self: Box<Self>,
        mut input: mpsc::Receiver<EventEnvelope>,
        output: mpsc::Sender<EventEnvelope>,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                maybe = input.recv() => {
                    let Some(mut event) = maybe else { break };
                    metrics::increment_counter!("events_in", "component" => self.name.clone());
                    self.apply_logic(&mut event.event);
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
    use chrono::Duration;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn sets_defaults_when_missing() {
        let event = EventEnvelope::new(Event::new());
        let t0 = Utc::now();
        let t =
            NormalizeSchemaTransform::new("test".into(), None, None, None, None, None, None, None);

        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);

        Box::new(t)
            .run(rx_in, tx_out, CancellationToken::new())
            .await;

        let event = rx_out.recv().await.expect("should output event");
        let log = event.event.as_log().unwrap();

        match log.fields.get("@timestamp") {
            Some(Value::Timestamp(ts)) => {
                assert!((*ts - t0) < Duration::seconds(5));
            }
            other => panic!("expected timestamp, got {:?}", other),
        }
        assert_eq!(
            log.fields.get("log_type"),
            Some(&Value::String("unknown".to_string()))
        );
        assert_eq!(
            log.fields.get("severity"),
            Some(&Value::String("INFO".to_string()))
        );
        assert_eq!(
            log.fields.get("soc_tenant"),
            Some(&Value::String("unknown".to_string()))
        );
    }

    #[tokio::test]
    async fn parses_timestamp_from_string_field_and_uses_default_log_type() {
        let mut event = Event::new();
        event.insert("ts_raw", "2024-01-02T03:04:05Z");
        let envelope = EventEnvelope::new(event);
        let t = NormalizeSchemaTransform::new(
            "test".into(),
            Some("ts_raw".to_string()),
            None,
            None,
            None,
            Some("message".to_string()),
            Some("custom".to_string()),
            None,
        );

        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(envelope).await.unwrap();
        drop(tx_in);

        Box::new(t)
            .run(rx_in, tx_out, CancellationToken::new())
            .await;

        let event = rx_out.recv().await.expect("should output event");
        let log = event.event.as_log().unwrap();

        match log.fields.get("@timestamp") {
            Some(Value::Timestamp(ts)) => {
                assert_eq!(ts.to_rfc3339(), "2024-01-02T03:04:05+00:00");
            }
            other => panic!("expected timestamp, got {:?}", other),
        }
        assert_eq!(
            log.fields.get("log_type"),
            Some(&Value::String("custom".to_string()))
        );
    }

    #[tokio::test]
    async fn normalizes_severity_from_string_and_number() {
        let mut event = Event::new();
        event.insert("sev", "warning");
        event.insert("sev_num", 2_i64);
        let envelope = EventEnvelope::new(event);

        let t = NormalizeSchemaTransform::new(
            "test".into(),
            None,
            None,
            Some("sev".to_string()),
            None,
            None,
            None,
            None,
        );

        // Simulating run
        // To test multiple transforms in sequence we have to recreate channels or test logic directly.
        // For simplicity, let's test `apply_logic` directly via run but one by one.

        // Run 1
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(envelope).await.unwrap();
        drop(tx_in);
        Box::new(t)
            .run(rx_in, tx_out, CancellationToken::new())
            .await;
        let event = rx_out.recv().await.unwrap();

        assert_eq!(
            event.event.as_log().unwrap().fields.get("severity"),
            Some(&Value::String("WARN".to_string()))
        );

        // Run 2 with different config on same event (which now has WARN severity, but we look at sev_num)
        let t2 = NormalizeSchemaTransform::new(
            "test2".into(),
            None,
            None,
            Some("sev_num".to_string()),
            None,
            None,
            None,
            None,
        );
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);
        Box::new(t2)
            .run(rx_in, tx_out, CancellationToken::new())
            .await;
        let event = rx_out.recv().await.unwrap();

        assert_eq!(
            event.event.as_log().unwrap().fields.get("severity"),
            Some(&Value::String("ERROR".to_string()))
        );
    }

    #[tokio::test]
    async fn maps_security_fields_and_tenant_default() {
        let mut event = Event::new();
        event.insert("client_ip", "1.1.1.1");
        event.insert("destination_port", 443_i64);
        event.insert("username", "alice");
        event.insert("verb", "GET");
        event.insert("status", "200");
        event.insert("rule_id", "R1");
        event.insert("signature", "TestRule");
        let envelope = EventEnvelope::new(event);

        let t = NormalizeSchemaTransform::new(
            "test".into(),
            None,
            None,
            None,
            None,
            Some("message".to_string()),
            Some("web".to_string()),
            Some("acme".to_string()),
        );

        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(envelope).await.unwrap();
        drop(tx_in);
        Box::new(t)
            .run(rx_in, tx_out, CancellationToken::new())
            .await;
        let event = rx_out.recv().await.unwrap();
        let log = event.event.as_log().unwrap();

        assert_eq!(
            log.fields.get("src_ip"),
            Some(&Value::String("1.1.1.1".into()))
        );
        assert_eq!(log.fields.get("dest_port"), Some(&Value::Integer(443)));
        assert_eq!(log.fields.get("user"), Some(&Value::String("alice".into())));
        assert_eq!(log.fields.get("action"), Some(&Value::String("GET".into())));
        assert_eq!(log.fields.get("result"), Some(&Value::String("200".into())));
        assert_eq!(log.fields.get("rule_id"), Some(&Value::String("R1".into())));
        assert_eq!(
            log.fields.get("rule_name"),
            Some(&Value::String("TestRule".into()))
        );
        assert_eq!(
            log.fields.get("soc_tenant"),
            Some(&Value::String("acme".into()))
        );
    }
}
