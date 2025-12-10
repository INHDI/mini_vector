use anyhow::Result;
use crate::event::{Event, Value};
use crate::transforms::Transform;

/// ScriptTransform – "mini VRL" rất đơn giản:
/// Hỗ trợ các câu lệnh:
///   .field = "literal"
///   .field = .other_field
///   .field = upcase(.other_field)
///   drop()
#[derive(Debug, Clone)]
pub enum ScriptOp {
    AssignLiteral { field: String, value: String },
    AssignField { field: String, src: String },
    UpcaseField { field: String, src: String },
    DropEvent,
}

pub struct ScriptTransform {
    pub ops: Vec<ScriptOp>,
}

impl ScriptTransform {
    pub fn compile(script: String) -> Result<Self> {
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

        Ok(Self { ops })
    }

    fn apply_op(&self, op: &ScriptOp, event: &mut Event) -> bool {
        match op {
            ScriptOp::AssignLiteral { field, value } => {
                event.insert(field.clone(), Value::String(value.clone()));
                true
            }
            ScriptOp::AssignField { field, src } => {
                if let Some(v) = event.fields.get(src).cloned() {
                    event.insert(field.clone(), v);
                } else {
                    event.insert(field.clone(), Value::String(String::new()));
                }
                true
            }
            ScriptOp::UpcaseField { field, src } => {
                if let Some(Value::String(s)) = event.fields.get(src) {
                    event.insert(field.clone(), Value::String(s.to_uppercase()));
                }
                true
            }
            ScriptOp::DropEvent => false,
        }
    }
}

impl Transform for ScriptTransform {
    fn apply(&self, event: &mut Event) -> bool {
        for op in &self.ops {
            if !self.apply_op(op, event) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_and_applies_ops() {
        let script = r#"
        .soc_tenant = "tenant01"
        .sev_up = upcase(.severity)
        "#;
        let t = ScriptTransform::compile(script.to_string()).unwrap();
        let mut event = Event::new();
        event.insert("severity", "warn");
        let keep = t.apply(&mut event);
        assert!(keep);
        assert_eq!(
            event.fields.get("soc_tenant"),
            Some(&Value::String("tenant01".to_string()))
        );
        assert_eq!(
            event.fields.get("sev_up"),
            Some(&Value::String("WARN".to_string()))
        );
    }

    #[test]
    fn drop_function_stops_event() {
        let script = r#"
        .x = "1"
        drop()
        .y = "2"
        "#;
        let t = ScriptTransform::compile(script.to_string()).unwrap();
        let mut event = Event::new();
        let keep = t.apply(&mut event);
        assert!(!keep);
        assert!(event.fields.get("x").is_some());
        assert!(event.fields.get("y").is_none());
    }

    #[test]
    fn invalid_script_fails_compile() {
        let script = ".bad upcase(\"x\")".to_string();
        assert!(ScriptTransform::compile(script).is_err());
    }
}
