use std::collections::HashMap;

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
    pub transforms: Option<HashMap<String, TransformConfig>>,
    pub router: Option<RouterConfig>,
    pub sinks: HashMap<String, SinkConfig>,
}
