use mini_vector::config::{FullConfig, SinkConfig, SourceConfig, TransformConfig, WhenFull};
use std::collections::HashMap;
use indexmap::IndexMap;

fn base_source() -> SourceConfig {
    SourceConfig {
        kind: "stdin".to_string(),
        address: None,
        path: None,
        include: vec![],
    }
}

fn console_sink_with_inputs(inputs: Vec<String>, when_full: WhenFull) -> SinkConfig {
    SinkConfig {
        kind: "console".to_string(),
        endpoint: None,
        inputs,
        buffer: Some(mini_vector::config::SinkBufferConfig {
            max_events: 1,
            when_full,
            ..mini_vector::config::SinkBufferConfig::default()
        }),
        batch: None,
        endpoints: vec![],
        mode: None,
        bulk: None,
        auth: None,
        tls: None,
        retry: None,
    }
}

fn json_transform(name: &str, inputs: Vec<String>) -> (String, TransformConfig) {
    (
        name.to_string(),
        TransformConfig {
            kind: "json_parse".to_string(),
            inputs,
            field: Some("message".to_string()),
            value: None,
            needle: None,
            drop_on_error: Some(false),
            remove_source: Some(true),
            error_field: Some("remap_error".to_string()),
            timestamp_field: None,
            host_field: None,
            severity_field: None,
            program_field: None,
            message_field: None,
            default_log_type: None,
            script: None,
            source: None,
            pattern: None,
            target_prefix: None,
        },
    )
}

#[tokio::test]
async fn inputs_topology_linear_chain() {
    let mut sources = HashMap::new();
    sources.insert("stdin".to_string(), base_source());

    let mut transforms = IndexMap::new();
    transforms.insert("parse".to_string(), json_transform("parse", vec!["stdin".into()]).1);

    let mut sinks = HashMap::new();
    sinks.insert(
        "console".to_string(),
        console_sink_with_inputs(vec!["parse".into()], WhenFull::Block),
    );

    let config = FullConfig {
        sources,
        transforms: Some(transforms),
        sinks,
    };

    // Just ensure validate passes and pipeline can start/stop without panic.
    config.validate().unwrap();
    // We won't run the pipeline fully (stdin blocks); just assert validation.
}
