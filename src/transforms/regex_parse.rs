use regex::Regex;
use tracing::warn;

use crate::event::{Event, Value};
use crate::transforms::Transform;

pub struct RegexParseTransform {
    pub field: String,
    pub regex: Regex,
    pub drop_on_error: bool,
    pub remove_source: bool,
}

impl RegexParseTransform {
    pub fn new(
        field: String,
        pattern: String,
        drop_on_error: bool,
        remove_source: bool,
    ) -> anyhow::Result<Self> {
        let regex = Regex::new(&pattern)
            .map_err(|e| anyhow::anyhow!("invalid regex pattern '{}': {}", pattern, e))?;
        Ok(Self {
            field,
            regex,
            drop_on_error,
            remove_source,
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
                    event.insert(name.to_string(), m.as_str().to_string());
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
            !self.drop_on_error
        }
    }
}
