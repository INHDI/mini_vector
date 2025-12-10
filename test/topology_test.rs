use mini_vector::config::{FullConfig, SinkConfig, SourceConfig, TransformConfig, WhenFull};
use mini_vector::event::{Event, Value};
use mini_vector::pipeline::run_pipeline;
use std::collections::HashMap;
use tokio::sync::mpsc;

fn base_source() -> SourceConfig {
    SourceConfig {
        kind: "stdin".to_string(),
        address: None,
        path: None,
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
        }),
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
            timestamp_field: None,
            host_field: None,
            severity_field: None,
            program_field: None,
            message_field: None,
            default_log_type: None,
            script: None,
            pattern: None,
            target_prefix: None,
        },
    )
}

#[tokio::test]
async fn inputs_topology_linear_chain() {
    let mut sources = HashMap::new();
    sources.insert("stdin".to_string(), base_source());

    let mut transforms = indexmap::IndexMap::new();
    transforms.insert("parse".to_string(), json_transform("parse", vec!["stdin".into()]).1);

    let mut sinks = HashMap::new();
    sinks.insert(
        "console".to_string(),
        console_sink_with_inputs(vec!["parse".into()], WhenFull::Block),
    );

    let config = FullConfig {
        sources,
        transforms: Some(transforms),
        router: None,
        sinks,
    };

    // Just ensure validate passes and pipeline can start/stop without panic.
    config.validate().unwrap();
    // We won't run the pipeline fully (stdin blocks); just assert validation.
}

#[tokio::test]
async fn drop_new_buffer_behavior() {
    // Simulate drop_new by using a small channel and try_send path.
    let (tx, mut rx) = mpsc::channel::<Event>(1);
    let ds = mini_vector::pipeline::Downstream {
        tx,
        mode: WhenFull::DropNew,
    };
    let mut event = Event::new();
    event.insert("x", 1);

    // Fill the channel
    ds.tx.try_send(event.clone()).unwrap();
    // This should drop
    mini_vector::pipeline::fan_out(event.clone(), &[ds]).await;
    // Only one message should be in the channel
    let first = rx.recv().await;
    let second = rx.try_recv();
    assert!(first.is_some());
    assert!(second.is_err());
}
