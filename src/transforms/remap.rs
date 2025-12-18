use anyhow::Result;
use async_trait::async_trait;
use metrics;
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use vrl::compiler::{self, TargetValueRef, TimeZone, runtime::Runtime, state::RuntimeState};
use vrl::stdlib;
use vrl::value::{KeyString, Secrets, Value as VrlValue};

use crate::event::{Event, EventEnvelope, LogEvent, Value};
use crate::transforms::Transform;

const DEFAULT_REMAP_TIMEOUT_MS: u64 = 100;

pub struct RemapTransform {
    name: String,
    program: vrl::compiler::Program,
    runtime: Runtime,
    drop_on_error: bool,
    error_field: Option<String>,
    timeout: Duration,
}

impl RemapTransform {
    pub fn new(
        name: String,
        source: String,
        drop_on_error: bool,
        error_field: Option<String>,
    ) -> Result<Self> {
        let compiled =
            compiler::compile(&source, &stdlib::all()).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(Self {
            name,
            program: compiled.program,
            runtime: Runtime::new(RuntimeState::default()),
            drop_on_error,
            error_field,
            timeout: Duration::from_millis(DEFAULT_REMAP_TIMEOUT_MS),
        })
    }

    fn process_one_inner(&mut self, event: &mut Event) -> Result<()> {
        let mut v = match event {
            Event::Log(LogEvent { fields }) => {
                // map log fields to VRL object
                let obj: BTreeMap<KeyString, VrlValue> = fields
                    .iter()
                    .map(|(k, v)| (k.clone().into(), to_vrl_value(v)))
                    .collect();
                VrlValue::Object(obj)
            }
        };

        let mut metadata = VrlValue::Object(BTreeMap::new());
        let mut secrets = Secrets::default();
        let mut target = TargetValueRef {
            value: &mut v,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };
        let tz = TimeZone::default();

        self.runtime
            .resolve(&mut target, &self.program, &tz)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // convert back to Event
        let Event::Log(LogEvent { fields }) = event;
        fields.clear();
        if let VrlValue::Object(obj) = v {
            for (k, v2) in obj {
                fields.insert(k.to_string(), from_vrl_value(&v2));
            }
        }

        Ok(())
    }

    async fn process_one(&mut self, event: &mut Event) -> Result<()> {
        let timeout = self.timeout;
        let fut = async {
            let res = std::panic::catch_unwind(AssertUnwindSafe(|| self.process_one_inner(event)));
            match res {
                Ok(r) => r,
                Err(_) => anyhow::bail!("remap panic"),
            }
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(res) => res,
            Err(_) => anyhow::bail!("remap timeout"),
        }
    }

    fn attach_error_field(&self, event: &mut Event, err: String) {
        let field = match &self.error_field {
            Some(f) if !f.is_empty() => f,
            _ => return,
        };

        let Event::Log(LogEvent { fields }) = event;
        if let Some(value) = fields.get_mut(field) {
            match value {
                Value::Array(arr) => {
                    arr.push(Value::String(err));
                }
                Value::String(s) => {
                    let existing = std::mem::take(s);
                    *value = Value::Array(vec![Value::String(existing), Value::String(err)]);
                }
                _ => {
                    let taken = std::mem::replace(value, Value::Null);
                    *value = Value::Array(vec![taken, Value::String(err)]);
                }
            }
        } else {
            fields.insert(field.clone(), Value::String(err));
        }
    }
}

fn to_vrl_value(v: &Value) -> VrlValue {
    match v {
        Value::Null => VrlValue::Null,
        Value::String(s) => VrlValue::from(s.clone()),
        Value::Integer(i) => VrlValue::from(*i),
        Value::Float(f) => VrlValue::from_f64_or_zero(*f),
        Value::Bool(b) => VrlValue::from(*b),
        Value::Timestamp(ts) => VrlValue::from(*ts),
        Value::Array(arr) => VrlValue::Array(arr.iter().map(to_vrl_value).collect()),
        Value::Object(obj) => {
            let mut map = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k.clone().into(), to_vrl_value(v));
            }
            VrlValue::Object(map)
        }
        Value::Bytes(b) => VrlValue::Bytes(b.clone().into()),
    }
}

fn from_vrl_value(v: &VrlValue) -> Value {
    match v {
        VrlValue::Null => Value::Null,
        VrlValue::Boolean(b) => Value::Bool(*b),
        VrlValue::Integer(i) => Value::Integer(*i),
        VrlValue::Float(f) => Value::Float(f.into_inner()), // ordered_float::NotNan
        VrlValue::Timestamp(ts) => Value::Timestamp(*ts),
        VrlValue::Bytes(b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                Value::String(s.to_string())
            } else {
                Value::Bytes(b.to_vec())
            }
        }
        VrlValue::Regex(r) => Value::String(r.as_str().to_string()), // fallback
        VrlValue::Array(arr) => Value::Array(arr.iter().map(from_vrl_value).collect()),
        VrlValue::Object(obj) => {
            let mut map = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k.to_string(), from_vrl_value(v));
            }
            Value::Object(map)
        }
    }
}

#[async_trait]
impl Transform for RemapTransform {
    async fn run(
        mut self: Box<Self>,
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

                    match self.process_one(&mut event.event).await {
                        Ok(()) => {
                            metrics::increment_counter!("events_out", "component" => self.name.clone());
                            match output.send(event).await {
                                Ok(_) => {}
                                Err(err) => {
                                    let ev = err.0;
                                    ev.ack.ack();
                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            let err_msg = err.to_string();
                            event.event.insert(
                                "transform_error".to_string(),
                                Value::String(err_msg.clone()),
                            );
                            event.event.insert(
                                "transform_error_stage".to_string(),
                                Value::String("remap".to_string()),
                            );
                            if self.drop_on_error {
                                tracing::warn!(target = "transform.remap", name = %self.name, %err_msg, "VRL transform failed");
                                metrics::increment_counter!(
                                    "events_dropped",
                                    "component" => self.name.clone(),
                                    "reason" => "vrl_error"
                                );
                                event.ack.ack();
                                continue;
                            } else {
                                tracing::warn!(target = "transform.remap", name = %self.name, %err_msg, "VRL transform failed, attaching error_field");
                                self.attach_error_field(&mut event.event, err_msg);
                                metrics::increment_counter!("events_out", "component" => self.name.clone());
                                match output.send(event).await {
                                    Ok(_) => {}
                                    Err(err) => {
                                        let ev = err.0;
                                        ev.ack.ack();
                                        break;
                                    }
                                }
                            }
                        }
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};
    use tokio_util::sync::CancellationToken;

    fn event_with_message(msg: &str) -> EventEnvelope {
        let mut ev = Event::new();
        ev.insert("message", msg.to_string());
        EventEnvelope::new(ev)
    }

    #[tokio::test]
    async fn remap_drops_on_error_when_configured() {
        let source = ". = parse_json!(.message)".to_string();
        let t =
            RemapTransform::new("remap1".into(), source, true, Some("remap_error".into())).unwrap();

        let (tx_in, rx_in) = mpsc::channel(4);
        let (tx_out, mut rx_out) = mpsc::channel(4);
        let shutdown = CancellationToken::new();

        tokio::spawn(async move {
            Box::new(t)
                .run(rx_in, tx_out, shutdown.clone())
                .await;
        });

        tx_in.send(event_with_message("hello")).await.unwrap();
        drop(tx_in);

        // Expect channel to close without yielding an event
        let res = timeout(Duration::from_millis(200), rx_out.recv()).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn remap_attaches_error_field_when_not_dropping() {
        let source = ". = parse_json!(.message)".to_string();
        let t = RemapTransform::new("remap2".into(), source, false, Some("remap_error".into()))
            .unwrap();

        let (tx_in, rx_in) = mpsc::channel(4);
        let (tx_out, mut rx_out) = mpsc::channel(4);
        let shutdown = CancellationToken::new();

        tokio::spawn(async move {
            Box::new(t)
                .run(rx_in, tx_out, shutdown.clone())
                .await;
        });

        tx_in.send(event_with_message("hello")).await.unwrap();
        drop(tx_in);

        let ev = timeout(Duration::from_millis(200), rx_out.recv())
            .await
            .expect("recv did not timeout")
            .expect("event should exist");

        let Event::Log(log) = ev.event;
        let err_val = log.fields.get("remap_error").expect("error field missing");
        match err_val {
            Value::String(s) => assert!(!s.is_empty()),
            Value::Array(arr) => {
                assert!(
                    arr.iter()
                        .any(|v| matches!(v, Value::String(s) if !s.is_empty()))
                );
            }
            _ => panic!("unexpected error field type"),
        }
    }
}
