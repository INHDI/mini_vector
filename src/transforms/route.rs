use async_trait::async_trait;
use metrics;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use vrl::compiler::{self, TargetValueRef, TimeZone, runtime::Runtime, state::RuntimeState};
use vrl::stdlib;
use vrl::value::{KeyString, Secrets, Value as VrlValue};

use crate::event::{Event, EventEnvelope, LogEvent, Value};
use crate::transforms::Transform;

pub struct RouteRule {
    pub name: String,
    pub outputs: Vec<String>,
    pub program: vrl::compiler::Program,
    pub runtime: Runtime,
}

pub struct RouteTransform {
    pub name: String,
    pub routes: Vec<RouteRule>,
    pub default_outputs: Vec<String>,
}

impl RouteTransform {
    pub fn new(
        name: String,
        routes_cfg: Vec<(String, String, Vec<String>)>,
        default_outputs: Vec<String>,
    ) -> anyhow::Result<Self> {
        let mut routes = Vec::new();
        for (route_name, condition, outputs) in routes_cfg {
            let wrapped = format!(".matched = ({})", condition);
            let compiled = compiler::compile(&wrapped, &stdlib::all())
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            routes.push(RouteRule {
                name: route_name,
                outputs,
                program: compiled.program,
                runtime: Runtime::new(RuntimeState::default()),
            });
        }
        Ok(Self {
            name,
            routes,
            default_outputs,
        })
    }

    fn eval_rule(&mut self, idx: usize, event: &Event) -> anyhow::Result<bool> {
        let rule = self.routes.get_mut(idx).expect("route index");
        let mut v = event_to_vrl(event);

        let mut metadata = VrlValue::Object(BTreeMap::new());
        let mut secrets = Secrets::default();
        let mut target = TargetValueRef {
            value: &mut v,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };
        let tz = TimeZone::default();

        if rule
            .runtime
            .resolve(&mut target, &rule.program, &tz)
            .is_ok()
            && let VrlValue::Object(obj) = v
            && let Some(VrlValue::Boolean(b)) = obj.get(&KeyString::from("matched"))
        {
            return Ok(*b);
        }
        Ok(false)
    }
}

#[async_trait]
impl Transform for RouteTransform {
    async fn run(
        mut self: Box<Self>,
        mut input: mpsc::Receiver<EventEnvelope>,
        output: mpsc::Sender<EventEnvelope>,
    ) {
        while let Some(event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            let mut any_matched = false;

            for idx in 0..self.routes.len() {
                match self.eval_rule(idx, &event.event) {
                    Ok(true) => {
                        any_matched = true;
                        let rule = &self.routes[idx];
                        for target in &rule.outputs {
                            let mut ev_clone = event.clone();
                            let Event::Log(LogEvent { fields }) = &mut ev_clone.event;
                            fields.insert(
                                "__route_target".to_string(),
                                Value::String(target.clone()),
                            );
                            match output.send(ev_clone).await {
                                Ok(_) => {
                                    metrics::increment_counter!(
                                        "events_out",
                                        "component" => self.name.clone(),
                                        "route" => rule.name.clone()
                                    );
                                }
                                Err(err) => {
                                    let ev = err.0;
                                    ev.ack.ack();
                                }
                            }
                        }
                    }
                    Ok(false) => {}
                    Err(err) => {
                        metrics::increment_counter!(
                            "events_dropped",
                            "component" => self.name.clone(),
                            "reason" => "route_eval_error"
                        );
                        tracing::warn!(target = "transform.route", name = %self.name, %err, "route evaluation error");
                    }
                }
            }

            if !any_matched {
                if !self.default_outputs.is_empty() {
                    for target in &self.default_outputs {
                        let mut ev_clone = event.clone();
                        let Event::Log(LogEvent { fields }) = &mut ev_clone.event;
                        fields.insert("__route_target".to_string(), Value::String(target.clone()));
                        match output.send(ev_clone).await {
                            Ok(_) => {
                                metrics::increment_counter!(
                                    "events_out",
                                    "component" => self.name.clone(),
                                    "route" => "default"
                                );
                            }
                            Err(err) => {
                                let ev = err.0;
                                ev.ack.ack();
                            }
                        }
                    }
                } else {
                    metrics::increment_counter!(
                        "events_dropped",
                        "component" => self.name.clone(),
                        "reason" => "no_route"
                    );
                }
            }
            event.ack.ack();
        }
    }
}

fn event_to_vrl(event: &Event) -> VrlValue {
    match event {
        Event::Log(LogEvent { fields }) => {
            let obj: BTreeMap<KeyString, VrlValue> = fields
                .iter()
                .map(|(k, v)| (k.clone().into(), to_vrl_value(v)))
                .collect();
            VrlValue::Object(obj)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    fn event_with_type(ty: &str) -> EventEnvelope {
        let mut ev = Event::new();
        ev.insert("log_type", ty.to_string());
        EventEnvelope::new(ev)
    }

    #[tokio::test]
    async fn routes_to_named_output_and_default() {
        let routes_cfg = vec![(
            "web".to_string(),
            ".log_type == \"web\"".to_string(),
            vec!["web_out".to_string()],
        )];
        let default_outputs = vec!["fallback".to_string()];
        let t = RouteTransform::new("router".into(), routes_cfg, default_outputs).unwrap();

        let (tx_in, rx_in) = mpsc::channel(4);
        let (tx_out, mut rx_out) = mpsc::channel(4);

        tokio::spawn(async move {
            Box::new(t).run(rx_in, tx_out).await;
        });

        tx_in.send(event_with_type("web")).await.unwrap();
        tx_in.send(event_with_type("other")).await.unwrap();
        drop(tx_in);

        let mut targets = Vec::new();
        let first = timeout(Duration::from_millis(200), rx_out.recv())
            .await
            .unwrap()
            .unwrap();
        let Event::Log(log) = &first.event;
        if let Some(Value::String(t)) = log.fields.get("__route_target") {
            targets.push(t.clone());
        }

        let second = timeout(Duration::from_millis(200), rx_out.recv())
            .await
            .unwrap()
            .unwrap();
        let Event::Log(log2) = &second.event;
        if let Some(Value::String(t)) = log2.fields.get("__route_target") {
            targets.push(t.clone());
        }

        assert!(targets.contains(&"web_out".to_string()));
        assert!(targets.contains(&"fallback".to_string()));
    }
}
