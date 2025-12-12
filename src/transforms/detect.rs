use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use async_trait::async_trait;
use metrics;
use serde::Deserialize;
use tokio::sync::mpsc;
use vrl::compiler::{self, runtime::Runtime, state::RuntimeState, TargetValueRef, TimeZone};
use vrl::stdlib;
use vrl::value::{KeyString, Secrets, Value as VrlValue};

use crate::event::{Event, Value, LogEvent};
use crate::transforms::Transform;

#[derive(Debug, Deserialize)]
pub struct DetectRule {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub condition: String, // VRL expression
    #[serde(default = "default_rule_severity")]
    pub severity: String,
}

fn default_rule_severity() -> String {
    "medium".to_string()
}

pub struct CompiledRule {
    pub meta: DetectRule,
    pub program: vrl::compiler::Program,
    pub runtime: Runtime,
}

pub struct DetectTransform {
    pub name: String,
    pub rules: Vec<CompiledRule>,
}

impl DetectTransform {
    pub fn from_rules_file(name: String, path: &str) -> anyhow::Result<Self> {
        let f = File::open(Path::new(path))?;
        let rules: Vec<DetectRule> = serde_yaml::from_reader(f)?;
        if rules.is_empty() {
            anyhow::bail!("detect rules file '{}' is empty", path);
        }
        let mut compiled = Vec::new();
        for r in rules {
            let wrapped = format!(".matched = ({})", r.condition);
            let program = compiler::compile(&wrapped, &stdlib::all())
                .map_err(|e| anyhow::anyhow!("rule '{}' compile error: {e:?}", r.id))?
                .program;
            compiled.push(CompiledRule {
                meta: r,
                program,
                runtime: Runtime::new(RuntimeState::default()),
            });
        }
        Ok(Self { name, rules: compiled })
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

    fn apply_rule(rule: &mut CompiledRule, event: &Event) -> anyhow::Result<bool> {
        let mut v = Self::event_to_vrl(event);
        let mut metadata = VrlValue::Object(BTreeMap::new());
        let mut secrets = Secrets::default();
        let mut target = TargetValueRef {
            value: &mut v,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };
        let tz = TimeZone::default();
        rule.runtime.resolve(&mut target, &rule.program, &tz)?;
        if let VrlValue::Object(obj) = target.value {
            if let Some(VrlValue::Boolean(b)) = obj.get(&KeyString::from("matched")) {
                return Ok(*b);
            }
        }
        Ok(false)
    }
}

#[async_trait]
impl Transform for DetectTransform {
    async fn run(mut self: Box<Self>, mut input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>) {
        while let Some(mut event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            for rule in &mut self.rules {
                match Self::apply_rule(rule, &event) {
                    Ok(true) => {
                        let Event::Log(LogEvent { fields }) = &mut event;
                        fields.insert("alert".to_string(), Value::Bool(true));
                        fields.insert("rule_id".to_string(), Value::String(rule.meta.id.clone()));
                        fields.insert("rule_name".to_string(), Value::String(rule.meta.name.clone()));
                        fields.insert("alert_severity".to_string(), Value::String(rule.meta.severity.clone()));
                        if !rule.meta.description.is_empty() {
                            fields.insert("rule_description".to_string(), Value::String(rule.meta.description.clone()));
                        }
                        metrics::increment_counter!(
                            "alerts_fired",
                            "component" => self.name.clone(),
                            "rule_id" => rule.meta.id.clone()
                        );
                        break;
                    }
                    Ok(false) => {}
                    Err(err) => {
                        metrics::increment_counter!(
                            "events_failed",
                            "component" => self.name.clone(),
                            "reason" => "rule_error"
                        );
                        tracing::warn!(target = "transform.detect", name = %self.name, %err, "rule evaluation failed");
                    }
                }
            }

            if output.send(event).await.is_err() {
                break;
            }
            metrics::increment_counter!("events_out", "component" => self.name.clone());
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
    use tempfile::NamedTempFile;
    use tokio::time::{timeout, Duration};

    fn write_rules(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        std::io::Write::write_all(f.as_file_mut(), content.as_bytes()).unwrap();
        f
    }

    #[tokio::test]
    async fn fires_alert_and_sets_fields() {
        let file = write_rules(
            r#"
- id: T1
  name: Test Rule
  severity: high
  condition: '.foo == "bar"'
"#,
        );
        let t = DetectTransform::from_rules_file("detect".into(), file.path().to_str().unwrap()).unwrap();

        let (tx_in, rx_in) = mpsc::channel(4);
        let (tx_out, mut rx_out) = mpsc::channel(4);
        tokio::spawn(async move { Box::new(t).run(rx_in, tx_out).await; });

        let mut ev = Event::new();
        ev.insert("foo", "bar");
        tx_in.send(ev).await.unwrap();
        drop(tx_in);

        let out = timeout(Duration::from_millis(200), rx_out.recv())
            .await
            .unwrap()
            .unwrap();
        let log = out.as_log().unwrap();
        assert_eq!(log.fields.get("alert"), Some(&Value::Bool(true)));
        assert_eq!(log.fields.get("rule_id"), Some(&Value::String("T1".into())));
        assert_eq!(log.fields.get("rule_name"), Some(&Value::String("Test Rule".into())));
        assert_eq!(log.fields.get("alert_severity"), Some(&Value::String("high".into())));
    }

    #[tokio::test]
    async fn does_not_alert_when_condition_false() {
        let file = write_rules(
            r#"
- id: T2
  name: NoMatch
  condition: '.foo == "bar"'
"#,
        );
        let t = DetectTransform::from_rules_file("detect".into(), file.path().to_str().unwrap()).unwrap();

        let (tx_in, rx_in) = mpsc::channel(4);
        let (tx_out, mut rx_out) = mpsc::channel(4);
        tokio::spawn(async move { Box::new(t).run(rx_in, tx_out).await; });

        let mut ev = Event::new();
        ev.insert("foo", "baz");
        tx_in.send(ev).await.unwrap();
        drop(tx_in);

        let out = timeout(Duration::from_millis(200), rx_out.recv())
            .await
            .unwrap()
            .unwrap();
        let log = out.as_log().unwrap();
        assert!(!matches!(log.fields.get("alert"), Some(Value::Bool(true))));
    }
}
