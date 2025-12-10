use std::collections::HashMap;

use indexmap::IndexMap;
use serde::Deserialize;

use crate::router::RouterConfig;

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub kind: String,

    // HTTP source config:
    #[serde(default)]
    pub address: Option<String>, // "0.0.0.0:9000"
    #[serde(default)]
    pub path: Option<String>, // "/ingest"
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "type")]
    pub kind: String,

    // add_field / contains_filter / json_parse
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub needle: Option<String>,

    // json_parse options
    #[serde(default)]
    pub drop_on_error: Option<bool>,
    #[serde(default)]
    pub remove_source: Option<bool>,

    // normalize_schema options
    #[serde(default)]
    pub timestamp_field: Option<String>,
    #[serde(default)]
    pub host_field: Option<String>,
    #[serde(default)]
    pub severity_field: Option<String>,
    #[serde(default)]
    pub program_field: Option<String>,
    #[serde(default)]
    pub message_field: Option<String>,
    #[serde(default)]
    pub default_log_type: Option<String>,

    // script transform
    #[serde(default)]
    pub script: Option<String>,

    #[serde(default)]
    pub pattern: Option<String>,

    #[serde(default)]
    pub target_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    #[serde(rename = "type")]
    pub kind: String,
    // HTTP sink config:
    #[serde(default)]
    pub endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FullConfig {
    pub sources: HashMap<String, SourceConfig>,
    pub transforms: Option<IndexMap<String, TransformConfig>>,
    pub router: Option<RouterConfig>,
    pub sinks: HashMap<String, SinkConfig>,
}

impl FullConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.sources.is_empty() {
            anyhow::bail!("config must contain at least one source");
        }
        if self.sinks.is_empty() {
            anyhow::bail!("config must contain at least one sink");
        }

        if let Some(router) = &self.router {
            if router.field.trim().is_empty() {
                anyhow::bail!("router.field cannot be empty");
            }
            for route in &router.routes {
                if !self.sinks.contains_key(&route.sink) {
                    anyhow::bail!("router route sink '{}' not defined in sinks", route.sink);
                }
            }
        }

        if let Some(transforms) = &self.transforms {
            for (name, t) in transforms {
                match t.kind.as_str() {
                    "regex_parse" => {
                        if t.field.as_deref().unwrap_or("").is_empty() {
                            anyhow::bail!("transform '{}' (regex_parse) missing 'field'", name);
                        }
                        if t.pattern.as_deref().unwrap_or("").is_empty() {
                            anyhow::bail!("transform '{}' (regex_parse) missing 'pattern'", name);
                        }
                    }
                    "json_parse" => {
                        if t.field.as_deref().unwrap_or("").is_empty() {
                            anyhow::bail!("transform '{}' (json_parse) missing 'field'", name);
                        }
                    }
                    "script" => {
                        if t.script.as_deref().unwrap_or("").is_empty() {
                            anyhow::bail!("transform '{}' (script) missing 'script'", name);
                        }
                    }
                    "contains_filter" => {
                        if t.field.as_deref().unwrap_or("").is_empty()
                            || t.needle.as_deref().unwrap_or("").is_empty()
                        {
                            anyhow::bail!(
                                "transform '{}' (contains_filter) requires 'field' and 'needle'",
                                name
                            );
                        }
                    }
                    "add_field" => {
                        if t.field.as_deref().unwrap_or("").is_empty()
                            || t.value.as_deref().unwrap_or("").is_empty()
                        {
                            anyhow::bail!(
                                "transform '{}' (add_field) requires 'field' and 'value'",
                                name
                            );
                        }
                    }
                    "normalize_schema" => { /* no required fields */ }
                    other => anyhow::bail!("unknown transform type '{}' for '{}'", other, name),
                }
            }
        }

        Ok(())
    }
}
