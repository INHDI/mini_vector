use regex::Regex;
use tracing::warn;

use crate::event::{Event, Value};
use crate::transforms::Transform;

pub struct RegexParseTransform {
    pub field: String,
    pub regex: Regex,
    pub drop_on_error: bool,
    pub remove_source: bool,
    pub target_prefix: Option<String>,
}

impl RegexParseTransform {
    pub fn new(
        field: String,
        pattern: String,
        drop_on_error: bool,
        remove_source: bool,
        target_prefix: Option<String>,
    ) -> anyhow::Result<Self> {
        let regex = Regex::new(&pattern)
            .map_err(|e| anyhow::anyhow!("invalid regex pattern '{}': {}", pattern, e))?;
        Ok(Self {
            field,
            regex,
            drop_on_error,
            remove_source,
            target_prefix,
        })
    }
}

impl Transform for RegexParseTransform {
    fn apply(&self, event: &mut Event) -> bool {
        let raw = match event.fields.get(&self.field) {
            Some(Value::String(s)) => s.clone(),
            _ => {
                if self.drop_on_error {
                    warn!(
                        "RegexParseTransform: source field '{}' missing or not string, dropping event",
                        self.field
                    );
                    return false;
                } else {
                    return true;
                }
            }
        };

        if raw.trim().is_empty() {
            return !self.drop_on_error;
        }

        if let Some(caps) = self.regex.captures(&raw) {
            for name in self
                .regex
                .capture_names()
                .flatten()
                .filter(|n| *n != "0")
            {
                if let Some(m) = caps.name(name) {
                    let key = if let Some(prefix) = &self.target_prefix {
                        format!("{}.{}", prefix, name)
                    } else {
                        name.to_string()
                    };
                    event.insert(key, m.as_str().to_string());
                }
            }

            if self.remove_source {
                event.fields.remove(&self.field);
            }

            true
        } else {
            warn!(
                "RegexParseTransform: regex did not match field '{}' value: {}",
                self.field, raw
            );
            if !self.drop_on_error {
                event.insert("regex_parse_error".to_string(), true);
            }
            !self.drop_on_error
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_named_groups_and_removes_source() {
        let mut event = Event::new();
        event.insert("message", "prog: hello");
        let t = RegexParseTransform::new(
            "message".into(),
            r"(?P<program>\w+): (?P<msg>.+)".into(),
            true,
            true,
            None,
        )
        .unwrap();
        let keep = t.apply(&mut event);
        assert!(keep);
        assert!(!event.fields.contains_key("message"));
        assert_eq!(
            event.fields.get("program"),
            Some(&Value::String("prog".to_string()))
        );
        assert_eq!(
            event.fields.get("msg"),
            Some(&Value::String("hello".to_string()))
        );
    }

    #[test]
    fn uses_target_prefix_and_sets_error_flag_on_non_drop() {
        let mut event = Event::new();
        event.insert("message", "no match");
        let t = RegexParseTransform::new(
            "message".into(),
            r"(?P<program>\w+): (?P<msg>.+)".into(),
            false,
            false,
            Some("parsed".to_string()),
        )
        .unwrap();
        let keep = t.apply(&mut event);
        assert!(keep);
        assert_eq!(
            event.fields.get("regex_parse_error"),
            Some(&Value::Bool(true))
        );
        assert!(event.fields.get("parsed.program").is_none());
    }
}
