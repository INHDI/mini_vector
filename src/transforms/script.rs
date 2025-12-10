use tracing::warn;

use crate::event::{Event, Value};
use crate::transforms::Transform;

/// ScriptTransform – "mini VRL" rất đơn giản:
/// Hỗ trợ các câu lệnh:
///   .field = "literal"
///   .field = .other_field
///   .field = upcase(.other_field)
pub struct ScriptTransform {
    pub lines: Vec<String>,
}

impl ScriptTransform {
    pub fn new(script: String) -> Self {
        let lines = script
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        Self { lines }
    }

    fn apply_line(&self, line: &str, event: &mut Event) {
        if !line.starts_with('.') {
            warn!(
                "ScriptTransform: invalid line (must start with '.'): {}",
                line
            );
            return;
        }
        let parts: Vec<&str> = line.splitn(2, '=').collect();
        if parts.len() != 2 {
            warn!("ScriptTransform: invalid assignment: {}", line);
            return;
        }
        let left = parts[0].trim();
        let right = parts[1].trim();

        if !left.starts_with('.') {
            warn!("ScriptTransform: invalid left side: {}", line);
            return;
        }
        let field_name = &left[1..];

        // .field = "literal"
        if right.starts_with('"') && right.ends_with('"') && right.len() >= 2 {
            let inner = &right[1..right.len() - 1];
            event.insert(field_name.to_string(), Value::String(inner.to_string()));
            return;
        }

        // .field = .other_field
        if right.starts_with('.') {
            let src = &right[1..];
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert(field_name.to_string(), v);
            } else {
                event.insert(field_name.to_string(), Value::String(String::new()));
            }
            return;
        }

        // .field = upcase(.other_field)
        if right.starts_with("upcase(") && right.ends_with(')') {
            let inner = &right["upcase(".len()..right.len() - 1];
            let inner = inner.trim();
            if inner.starts_with('.') {
                let src = &inner[1..];
                if let Some(Value::String(s)) = event.fields.get(src) {
                    event.insert(field_name.to_string(), Value::String(s.to_uppercase()));
                }
            }
            return;
        }

        warn!("ScriptTransform: unsupported expression: {}", line);
    }
}

impl Transform for ScriptTransform {
    fn apply(&self, event: &mut Event) -> bool {
        for line in &self.lines {
            self.apply_line(line, event);
        }
        true
    }
}
