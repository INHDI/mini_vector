use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::event::{Event, Value};
use crate::transforms::Transform;

/// ScriptTransform – "mini VRL" rất đơn giản
#[derive(Debug, Clone)]
pub enum ScriptOp {
    AssignLiteral { field: String, value: String },
    AssignField { field: String, src: String },
    UpcaseField { field: String, src: String },
    DropEvent,
}

pub struct ScriptTransform {
    pub name: String,
    pub ops: Vec<ScriptOp>,
}

impl ScriptTransform {
    pub fn compile(name: String, script: String) -> Result<Self> {
        let mut ops = Vec::new();
        for raw_line in script.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if line == "drop()" {
                ops.push(ScriptOp::DropEvent);
                continue;
            }

            if !line.starts_with('.') {
                anyhow::bail!("invalid line (must start with '.'): {}", line);
            }
            let parts: Vec<&str> = line.splitn(2, '=').collect();
            if parts.len() != 2 {
                anyhow::bail!("invalid assignment: {}", line);
            }
            let left = parts[0].trim();
            let right = parts[1].trim();

            if !left.starts_with('.') {
                anyhow::bail!("invalid left side: {}", line);
            }
            let field_name = left[1..].to_string();

            // .field = "literal"
            if right.starts_with('"') && right.ends_with('"') && right.len() >= 2 {
                let inner = &right[1..right.len() - 1];
                ops.push(ScriptOp::AssignLiteral {
                    field: field_name,
                    value: inner.to_string(),
                });
                continue;
            }

            // .field = .other_field
            if right.starts_with('.') {
                let src = right[1..].to_string();
                ops.push(ScriptOp::AssignField {
                    field: field_name,
                    src,
                });
                continue;
            }

            // .field = upcase(.other_field)
            if right.starts_with("upcase(") && right.ends_with(')') {
                let inner = &right["upcase(".len()..right.len() - 1];
                let inner = inner.trim();
                if !inner.starts_with('.') {
                    anyhow::bail!("upcase() argument must be a field: {}", line);
                }
                let src = inner[1..].to_string();
                ops.push(ScriptOp::UpcaseField {
                    field: field_name,
                    src,
                });
                continue;
            }

            anyhow::bail!("unsupported expression: {}", line);
        }

        Ok(Self { name, ops })
    }

    fn apply_op(&self, op: &ScriptOp, event: &mut Event) -> bool {
        let log = match event {
            Event::Log(l) => l,
        };

        match op {
            ScriptOp::AssignLiteral { field, value } => {
                log.fields.insert(field.clone(), Value::String(value.clone()));
                true
            }
            ScriptOp::AssignField { field, src } => {
                if let Some(v) = log.fields.get(src).cloned() {
                    log.fields.insert(field.clone(), v);
                } else {
                    log.fields.insert(field.clone(), Value::String(String::new()));
                }
                true
            }
            ScriptOp::UpcaseField { field, src } => {
                if let Some(Value::String(s)) = log.fields.get(src) {
                    log.fields.insert(field.clone(), Value::String(s.to_uppercase()));
                }
                true
            }
            ScriptOp::DropEvent => false,
        }
    }
}

#[async_trait]
impl Transform for ScriptTransform {
    async fn run(self: Box<Self>, mut input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>) {
        while let Some(mut event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            
            let mut keep = true;
            for op in &self.ops {
                if !self.apply_op(op, &mut event) {
                    keep = false;
                    break;
                }
            }

            if keep {
                 if output.send(event).await.is_err() { break; }
                 metrics::increment_counter!("events_out", "component" => self.name.clone());
            } else {
                 metrics::increment_counter!("events_dropped", "component" => self.name.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn compiles_and_applies_ops() {
        let script = r#"
        .soc_tenant = "tenant01"
        .sev_up = upcase(.severity)
        "#;
        let t = ScriptTransform::compile("test".into(), script.to_string()).unwrap();
        let mut event = Event::new();
        event.insert("severity", "warn");
        
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);

        Box::new(t).run(rx_in, tx_out).await;
        
        let event = rx_out.recv().await.expect("should output event");
        let log = event.as_log().unwrap();

        assert_eq!(
            log.fields.get("soc_tenant"),
            Some(&Value::String("tenant01".to_string()))
        );
        assert_eq!(
            log.fields.get("sev_up"),
            Some(&Value::String("WARN".to_string()))
        );
    }

    #[tokio::test]
    async fn drop_function_stops_event() {
        let script = r#"
        .x = "1"
        drop()
        .y = "2"
        "#;
        let t = ScriptTransform::compile("test".into(), script.to_string()).unwrap();
        let event = Event::new();
        
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);

        Box::new(t).run(rx_in, tx_out).await;
        
        assert!(rx_out.recv().await.is_none());
    }

    #[test]
    fn invalid_script_fails_compile() {
        let script = ".bad upcase(\"x\")".to_string();
        assert!(ScriptTransform::compile("test".into(), script).is_err());
    }
}
