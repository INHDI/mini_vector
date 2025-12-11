use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub kind: String,

    // HTTP source config:
    #[serde(default)]
    pub address: Option<String>, // "0.0.0.0:9000"
    #[serde(default)]
    pub path: Option<String>, // "/ingest"

    // File source config:
    #[serde(default)]
    pub include: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "type")]
    pub kind: String,

    #[serde(default)]
    pub inputs: Vec<String>,

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
    #[serde(default)]
    pub inputs: Vec<String>,
    // HTTP sink config:
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub buffer: Option<SinkBufferConfig>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,

    // OpenSearch/Elasticsearch sink config:
    #[serde(default)]
    pub endpoints: Vec<String>,
    #[serde(default)]
    pub mode: Option<String>, // "bulk" supported
    #[serde(default)]
    pub bulk: Option<OpenSearchBulkConfig>,
    #[serde(default)]
    pub auth: Option<OpenSearchAuthConfig>,
    #[serde(default)]
    pub tls: Option<OpenSearchTlsConfig>,
    #[serde(default)]
    pub retry: Option<OpenSearchRetryConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchConfig {
    #[serde(default = "default_batch_size")]
    pub max_events: usize,
    #[serde(default = "default_batch_timeout")]
    pub timeout_secs: u64,
}

fn default_batch_size() -> usize {
    10
}

fn default_batch_timeout() -> u64 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenSearchBulkConfig {
    pub index: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenSearchAuthConfig {
    #[serde(default)]
    pub strategy: String, // "basic"
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenSearchTlsConfig {
    #[serde(default = "default_verify_true")]
    pub verify_certificate: bool,
    #[serde(default = "default_verify_true")]
    pub verify_hostname: bool,
}

fn default_verify_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenSearchRetryConfig {
    #[serde(default = "default_retry_attempts")]
    pub attempts: u32,
    #[serde(default = "default_retry_backoff_secs")]
    pub backoff_secs: u64,
    #[serde(default = "default_retry_backoff_secs")]
    pub max_backoff_secs: u64,
}

fn default_retry_attempts() -> u32 {
    3
}
fn default_retry_backoff_secs() -> u64 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkBufferConfig {
    #[serde(default = "default_buffer_size")]
    pub max_events: usize,
    #[serde(default)]
    pub when_full: WhenFull,
}

fn default_buffer_size() -> usize {
    1024
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WhenFull {
    Block,
    DropNew,
}

impl Default for WhenFull {
    fn default() -> Self {
        WhenFull::Block
    }
}

#[derive(Debug, Deserialize)]
pub struct FullConfig {
    pub sources: HashMap<String, SourceConfig>,
    pub transforms: Option<IndexMap<String, TransformConfig>>,
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

        let mut node_names: HashSet<String> = self.sources.keys().cloned().collect();
        if let Some(transforms) = &self.transforms {
            node_names.extend(transforms.keys().cloned());
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

        // Validate inputs topology if provided
        let has_inputs = self
            .transforms
            .as_ref()
            .map(|t| t.values().any(|cfg| !cfg.inputs.is_empty()))
            .unwrap_or(false)
            || self
                .sinks
                .values()
                .any(|cfg| !cfg.inputs.is_empty());

        if has_inputs {
            if let Some(transforms) = &self.transforms {
                for (name, t) in transforms {
                    if t.inputs.is_empty() {
                        anyhow::bail!("transform '{}' uses inputs mode but 'inputs' is empty", name);
                    }
                    for inp in &t.inputs {
                        if !node_names.contains(inp) {
                            anyhow::bail!("transform '{}' input '{}' not found", name, inp);
                        }
                    }
                }
            }

            for (sink_name, sink_cfg) in &self.sinks {
                if sink_cfg.inputs.is_empty() {
                    anyhow::bail!("sink '{}' uses inputs mode but 'inputs' is empty", sink_name);
                }
                for inp in &sink_cfg.inputs {
                    if !node_names.contains(inp) {
                        anyhow::bail!("sink '{}' input '{}' not found", sink_name, inp);
                    }
                }
            }
        }

        // Validate sink buffers
        for (sink_name, sink_cfg) in &self.sinks {
            if let Some(buf) = &sink_cfg.buffer {
                if buf.max_events == 0 {
                    anyhow::bail!("sink '{}' buffer.max_events must be > 0", sink_name);
                }
            }
            if let Some(batch) = &sink_cfg.batch {
                if batch.max_events == 0 {
                    anyhow::bail!("sink '{}' batch.max_events must be > 0", sink_name);
                }
                if batch.timeout_secs == 0 {
                    anyhow::bail!("sink '{}' batch.timeout_secs must be > 0", sink_name);
                }
            }

            if sink_cfg.kind == "opensearch" || sink_cfg.kind == "elasticsearch" {
                if sink_cfg.endpoints.is_empty() {
                    anyhow::bail!("sink '{}' (opensearch) requires non-empty endpoints", sink_name);
                }
                let mode = sink_cfg.mode.clone().unwrap_or_else(|| "bulk".to_string());
                if mode != "bulk" {
                    anyhow::bail!("sink '{}' (opensearch) only supports mode='bulk' for now", sink_name);
                }
                let bulk = sink_cfg
                    .bulk
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("sink '{}' (opensearch) missing bulk config", sink_name))?;
                if bulk.index.trim().is_empty() {
                    anyhow::bail!("sink '{}' (opensearch) bulk.index cannot be empty", sink_name);
                }

                if let Some(retry) = &sink_cfg.retry {
                    if retry.attempts == 0 {
                        anyhow::bail!("sink '{}' (opensearch) retry.attempts must be >= 1", sink_name);
                    }
                    if retry.backoff_secs == 0 {
                        anyhow::bail!("sink '{}' (opensearch) retry.backoff_secs must be >= 1", sink_name);
                    }
                    if retry.max_backoff_secs < retry.backoff_secs {
                        anyhow::bail!("sink '{}' (opensearch) retry.max_backoff_secs must be >= backoff_secs", sink_name);
                    }
                }
            }
        }

        // Detect orphan nodes (no downstream) when using inputs
        if has_inputs {
            let mut has_downstream: HashSet<String> = HashSet::new();
            if let Some(transforms) = &self.transforms {
                for (_, t) in transforms {
                    for inp in &t.inputs {
                        has_downstream.insert(inp.clone());
                    }
                }
            }
            for (_, sink_cfg) in &self.sinks {
                for inp in &sink_cfg.inputs {
                    has_downstream.insert(inp.clone());
                }
            }
            for source in self.sources.keys() {
                if !has_downstream.contains(source) {
                    anyhow::bail!("source '{}' has no downstream in inputs topology", source);
                }
            }
        }

        // Detect cycles with DFS
        if has_inputs {
            let mut graph: HashMap<String, Vec<String>> = HashMap::new();
            if let Some(transforms) = &self.transforms {
                for (name, t) in transforms {
                    graph.insert(name.clone(), t.inputs.clone());
                }
            }
            for (sink_name, sink_cfg) in &self.sinks {
                graph.insert(format!("sink:{}", sink_name), sink_cfg.inputs.clone());
            }

            fn dfs(
                node: &str,
                graph: &HashMap<String, Vec<String>>,
                visiting: &mut HashSet<String>,
                visited: &mut HashSet<String>,
            ) -> anyhow::Result<()> {
                if visited.contains(node) {
                    return Ok(());
                }
                if !visiting.insert(node.to_string()) {
                    anyhow::bail!("cycle detected at node '{}'", node);
                }
                if let Some(neigh) = graph.get(node) {
                    for n in neigh {
                        dfs(n, graph, visiting, visited)?;
                    }
                }
                visiting.remove(node);
                visited.insert(node.to_string());
                Ok(())
            }

            let mut visiting = HashSet::new();
            let mut visited = HashSet::new();
            for node in graph.keys() {
                dfs(node, &graph, &mut visiting, &mut visited)?;
            }
        }

        Ok(())
    }
}
