use crate::event::{Event, Value};
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

impl Transform for NormalizeSchemaTransform {
    fn apply(&self, event: &mut Event) -> bool {
        if let Some(ref src) = self.timestamp_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("@timestamp".to_string(), v);
            }
        }
        if let Some(ref src) = self.host_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("host".to_string(), v);
            }
        }
        if let Some(ref src) = self.severity_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("severity".to_string(), v);
            }
        }
        if let Some(ref src) = self.program_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("program".to_string(), v);
            }
        }
        if let Some(ref src) = self.message_field {
            if let Some(v) = event.fields.get(src).cloned() {
                event.insert("message".to_string(), v);
            }
        }

        if !event.fields.contains_key("log_type") {
            if let Some(ref lt) = self.default_log_type {
                event.insert("log_type".to_string(), Value::String(lt.clone()));
            }
        }

        true
    }
}
