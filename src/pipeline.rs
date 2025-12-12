use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tracing::{info, warn};
use metrics;

use crate::config::{
    FullConfig, SinkBufferConfig, SinkConfig, SourceConfig, TransformConfig, WhenFull,
};
use crate::event::{Event, Value};
// Router is deprecated in DAG mode, but we might keep it for legacy compat if we wanted to map it to a "RouterTransform"
// use crate::router::{EventRouter, RouterConfig};
use crate::sinks::console::ConsoleSink;
use crate::sinks::file::FileSink;
use crate::sinks::http::HttpSink;
use crate::sinks::opensearch::OpenSearchSink;
use crate::sinks::Sink;
use crate::sources::file::FileSource;
use crate::sources::http::HttpSource;
use crate::sources::syslog::SyslogSource;
use crate::sources::tcp::TcpSource;
use crate::sources::stdin::StdinSource;
use crate::sources::Source;
use crate::transforms::add_field::AddFieldTransform;
use crate::transforms::contains_filter::ContainsFilterTransform;
use crate::transforms::json_parse::JsonParseTransform;
use crate::transforms::normalize_schema::NormalizeSchemaTransform;
use crate::transforms::regex_parse::RegexParseTransform;
use crate::transforms::script::ScriptTransform;
use crate::transforms::Transform;
use crate::transforms::remap::RemapTransform;
use crate::transforms::route::RouteTransform;
use crate::transforms::detect::DetectTransform;


const DEFAULT_CHANNEL_SIZE: usize = 1024;

fn build_source(name: &str, cfg: &SourceConfig) -> anyhow::Result<Box<dyn Source>> {
    match cfg.kind.as_str() {
        "stdin" => Ok(Box::new(StdinSource::new(name.to_string()))),
        "file" => Ok(Box::new(FileSource::new(name.to_string(), cfg.include.clone()))),
        "http" => {
            let addr_str = cfg
                .address
                .clone()
                .ok_or_else(|| anyhow::anyhow!("source '{}' (http) missing 'address'", name))?;

            let path = cfg
                .path
                .clone()
                .unwrap_or_else(|| "/ingest".to_string());

            let addr: SocketAddr = addr_str.parse().map_err(|e| {
                anyhow::anyhow!(
                    "invalid address '{}' for source '{}': {}",
                    addr_str,
                    name,
                    e
                )
            })?;

            Ok(Box::new(HttpSource::new(name.to_string(), addr, path)))
        }
        "syslog" => {
            let addr = cfg
                .address
                .clone()
                .unwrap_or_else(|| format!("0.0.0.0:{}", cfg.port.unwrap_or(514)));
            let mode = cfg.mode.clone().unwrap_or_else(|| "udp".to_string());
            let max_length = cfg.max_length.unwrap_or(65535);
            Ok(Box::new(SyslogSource::new(
                name.to_string(),
                mode,
                addr,
                max_length,
            )))
        }
        "tcp" => {
            let addr = cfg
                .address
                .clone()
                .unwrap_or_else(|| format!("0.0.0.0:{}", cfg.port.unwrap_or(9000)));
            let max_length = cfg.max_length.unwrap_or(65535);
            Ok(Box::new(TcpSource::new(name.to_string(), addr, max_length)))
        }
        other => anyhow::bail!("Unknown source type '{}' for '{}'", other, name),
    }
}

fn build_transform(name: &str, cfg: &TransformConfig) -> anyhow::Result<Box<dyn Transform>> {
    let name_owned = name.to_string();
    match cfg.kind.as_str() {
        "add_field" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'field'", name))?;
            let value = cfg
                .value
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'value'", name))?;
            Ok(Box::new(AddFieldTransform::new(
                name_owned,
                field,
                Value::from(value),
            )))
        }
        "contains_filter" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'field'", name))?;
            let needle = cfg
                .needle
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' missing 'needle'", name))?;
            Ok(Box::new(ContainsFilterTransform::new(name_owned, field, needle)))
        }
        "json_parse" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (json_parse) missing 'field'", name))?;
            let drop_on_error = cfg.drop_on_error.unwrap_or(false);
            let remove_source = cfg.remove_source.unwrap_or(false);
            let target_prefix = cfg.target_prefix.clone();
            Ok(Box::new(JsonParseTransform::new(
                name_owned,
                field,
                drop_on_error,
                remove_source,
                target_prefix,
            )))
        }
        "normalize_schema" => {
            Ok(Box::new(NormalizeSchemaTransform::new(
                name_owned,
                cfg.timestamp_field.clone(),
                cfg.host_field.clone(),
                cfg.severity_field.clone(),
                cfg.program_field.clone(),
                cfg.message_field.clone(),
                cfg.default_log_type.clone(),
                cfg.default_tenant.clone(),
            )))
        }
        "script" => {
            let script = cfg
                .script
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (script) missing 'script'", name))?;
            let t = ScriptTransform::compile(name_owned, script)?;
            Ok(Box::new(t))
        }
        "regex_parse" => {
            let field = cfg
                .field
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (regex_parse) missing 'field'", name))?;
            let pattern = cfg
                .pattern
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (regex_parse) missing 'pattern'", name))?;

            let drop_on_error = cfg.drop_on_error.unwrap_or(false);
            let remove_source = cfg.remove_source.unwrap_or(false);
            let target_prefix = cfg.target_prefix.clone();

            let t = RegexParseTransform::new(name_owned, field, pattern, drop_on_error, remove_source, target_prefix)?;
            Ok(Box::new(t))
        }
        "remap" => {
            let source = cfg
                .source
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (remap) missing 'source'", name))?;
            let drop_on_error = cfg.drop_on_error.unwrap_or(true);
            let error_field = cfg
                .error_field
                .clone()
                .or_else(|| Some("remap_error".to_string()));
            let t = RemapTransform::new(name_owned, source, drop_on_error, error_field)?;
            Ok(Box::new(t))
        }
        "route" => {
            let routes_cfg = cfg
                .routes
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (route) missing routes", name))?;
            let mut compiled = Vec::new();
            let mut default_outputs = Vec::new();
            for (rname, rcfg) in routes_cfg {
                if rname == "default" {
                    default_outputs = rcfg.outputs.clone();
                    continue;
                }
                let condition = rcfg
                    .condition
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("transform '{}' route '{}' missing condition", name, rname))?;
                compiled.push((rname, condition, rcfg.outputs.clone()));
            }
            let t = RouteTransform::new(name_owned, compiled, default_outputs)?;
            Ok(Box::new(t))
        }
        "detect" => {
            let path = cfg
                .rules_path
                .clone()
                .ok_or_else(|| anyhow::anyhow!("transform '{}' (detect) missing 'rules_path'", name))?;
            let t = DetectTransform::from_rules_file(name_owned, &path, cfg.alert_outputs.clone())?;
            Ok(Box::new(t))
        }
        other => anyhow::bail!("Unknown transform type '{}' for '{}'", other, name),
    }
}

async fn build_sink(
    name: &str,
    cfg: &SinkConfig,
    health: &crate::health::HealthState,
) -> anyhow::Result<Box<dyn Sink>> {
    match cfg.kind.as_str() {
        "console" => Ok(Box::new(ConsoleSink::new(name.to_string()))),
        "http" => {
            let endpoint = cfg
                .endpoint
                .clone()
                .ok_or_else(|| anyhow::anyhow!("sink '{}' (http) missing 'endpoint'", name))?;
            Ok(Box::new(HttpSink::new(
                name.to_string(),
                endpoint,
                cfg.batch.clone(),
            )))
        }
        "opensearch" | "elasticsearch" => {
            let endpoints = cfg.endpoints.clone();
            let mode = cfg.mode.clone().unwrap_or_else(|| "bulk".to_string());
            let bulk = cfg
                .bulk
                .clone()
                .ok_or_else(|| anyhow::anyhow!("sink '{}' (opensearch) missing bulk config", name))?;
            let sink = OpenSearchSink::new(
                name.to_string(),
                endpoints,
                mode,
                bulk,
                cfg.auth.clone(),
                cfg.tls.clone(),
                cfg.batch.clone(),
                cfg.retry.clone(),
                Some(health.clone()),
            )?;
            Ok(Box::new(sink))
        }
        "file" => {
            let path = cfg
                .path
                .clone()
                .ok_or_else(|| anyhow::anyhow!("sink '{}' (file) missing 'path'", name))?;
            let max_bytes = cfg.max_bytes.unwrap_or(10 * 1024 * 1024); // 10MB default
            let sink = FileSink::new(name.to_string(), path, max_bytes).await?;
            Ok(Box::new(sink))
        }
        other => anyhow::bail!("Unknown sink type '{}' for '{}'", other, name),
    }
}

#[derive(Clone)]
struct Downstream {
    name: String,
    tx: mpsc::Sender<Event>,
    mode: WhenFull,
}

async fn fan_out(event: Event, downstreams: &[Downstream]) {
    let mut event = event;
    let mut target: Option<String> = None;
    let Event::Log(log) = &mut event;
    if let Some(Value::String(route)) = log.fields.remove("__route_target") {
        target = Some(route);
    }

    for ds in downstreams {
        if let Some(ref t) = target {
            if ds.name != *t {
                continue;
            }
        }
        match ds.mode {
            WhenFull::Block => {
                if let Err(err) = ds.tx.send(event.clone()).await {
                    warn!("downstream send failed: {}", err);
                }
            }
            WhenFull::DropNew => {
                if let Err(err) = ds.tx.try_send(event.clone()) {
                    warn!("downstream drop_new: {}", err);
                    metrics::increment_counter!(
                        "events_dropped",
                        "component" => ds.name.clone(),
                        "reason" => "buffer_full"
                    );
                }
            }
        }
    }
}

pub async fn run_pipeline(
    config: FullConfig,
    external_shutdown: Option<broadcast::Receiver<()>>,
    health: crate::health::HealthState,
) -> anyhow::Result<()> {
    // Shutdown signal
    let (shutdown_tx, _) = broadcast::channel(1);

    // Spawn signal handler
    let shutdown_tx_clone = shutdown_tx.clone();
    task::spawn(async move {
        if let Ok(_) = tokio::signal::ctrl_c().await {
            info!("Received Ctrl+C, shutting down...");
            let _ = shutdown_tx_clone.send(());
        }
    });

    // External shutdown (reload/drain)
    if let Some(mut rx) = external_shutdown {
        let shutdown_tx_external = shutdown_tx.clone();
        task::spawn(async move {
            if rx.recv().await.is_ok() {
                let _ = shutdown_tx_external.send(());
            }
        });
    }

    // If no inputs are defined, we technically have a disconnected graph,
    // but we proceed anyway (maybe only sources running).
    // The previous run_pipeline_linear is removed as it's incompatible with async Transform::run.
    
    // Build channels for transforms and sinks
    let mut transform_channels: HashMap<String, mpsc::Sender<Event>> = HashMap::new();
    let mut transform_receivers: HashMap<String, mpsc::Receiver<Event>> = HashMap::new();
    if let Some(transforms) = &config.transforms {
        for name in transforms.keys() {
            let (tx, rx) = mpsc::channel::<Event>(DEFAULT_CHANNEL_SIZE);
            transform_channels.insert(name.clone(), tx);
            transform_receivers.insert(name.clone(), rx);
        }
    }

    let mut sink_channels: HashMap<String, (mpsc::Sender<Event>, WhenFull)> = HashMap::new();
    let mut sink_receivers: HashMap<String, mpsc::Receiver<Event>> = HashMap::new();
    for (name, scfg) in &config.sinks {
        let buffer: SinkBufferConfig = scfg
            .buffer
            .clone()
            .unwrap_or_else(|| SinkBufferConfig {
                max_events: DEFAULT_CHANNEL_SIZE,
                ..SinkBufferConfig::default()
            });
        let (tx, rx) = mpsc::channel::<Event>(buffer.max_events);
        sink_channels.insert(name.clone(), (tx, buffer.when_full));
        sink_receivers.insert(name.clone(), rx);
    }

    // Build downstream adjacency
    let mut downstreams: HashMap<String, Vec<Downstream>> = HashMap::new();
    if let Some(transforms) = &config.transforms {
        for (name, cfg) in transforms {
            for inp in &cfg.inputs {
                let entry = downstreams.entry(inp.clone()).or_default();
                if let Some(tx) = transform_channels.get(name) {
                    entry.push(Downstream {
                        name: name.clone(),
                        tx: tx.clone(),
                        mode: WhenFull::Block,
                    });
                }
            }
        }
    }
    for (sink_name, sink_cfg) in &config.sinks {
        for inp in &sink_cfg.inputs {
            let entry = downstreams.entry(inp.clone()).or_default();
            if let Some((tx, mode)) = sink_channels.get(sink_name) {
                entry.push(Downstream {
                    name: sink_name.clone(),
                    tx: tx.clone(),
                    mode: *mode,
                });
            }
        }
    }

    // Route transform explicit outputs
    if let Some(transforms) = &config.transforms {
        for (name, cfg) in transforms {
            if cfg.kind == "route" {
                if let Some(routes) = &cfg.routes {
                    for (_rname, rcfg) in routes {
                        for out in &rcfg.outputs {
                            let entry = downstreams.entry(name.clone()).or_default();
                            if let Some(tx) = transform_channels.get(out) {
                                entry.push(Downstream {
                                    name: out.clone(),
                                    tx: tx.clone(),
                                    mode: WhenFull::Block,
                                });
                            } else if let Some((tx, mode)) = sink_channels.get(out) {
                                entry.push(Downstream {
                                    name: out.clone(),
                                    tx: tx.clone(),
                                    mode: *mode,
                                });
                            } else {
                                warn!("route transform '{}' targets unknown output '{}'", name, out);
                            }
                        }
                    }
                }
            }
        }
    }

    // Spawn sink tasks
    let mut sink_tasks = Vec::new();
    for (name, scfg) in &config.sinks {
        let sink = build_sink(name, scfg, &health).await?;
        if let Some(rx) = sink_receivers.remove(name) {
            sink_tasks.push(task::spawn(async move {
                sink.run(rx).await;
            }));
        }
    }

    // Spawn transform tasks
    let mut transform_tasks = Vec::new();
    if let Some(transforms) = &config.transforms {
        for (name, cfg) in transforms {
            let t = build_transform(name, cfg)?;
            if let Some(rx) = transform_receivers.remove(name) {
                let downs = downstreams.get(name).cloned().unwrap_or_default();
                
                // Create intermediate channel for Transform output -> FanOut
                let (tx_out, mut rx_out) = mpsc::channel::<Event>(DEFAULT_CHANNEL_SIZE);
                
                transform_tasks.push(task::spawn(async move {
                    t.run(rx, tx_out).await;
                }));

                // FanOut task
                transform_tasks.push(task::spawn(async move {
                    while let Some(event) = rx_out.recv().await {
                         fan_out(event, &downs).await;
                    }
                }));
            }
        }
    }

    // Spawn source tasks + fanout
    let mut source_tasks = Vec::new();
    for (name, scfg) in &config.sources {
        let src = build_source(name, scfg)?;
        let downs = downstreams.get(name).cloned().unwrap_or_default();
        let (tx, mut rx) = mpsc::channel::<Event>(DEFAULT_CHANNEL_SIZE);
        
        let shutdown_rx = shutdown_tx.subscribe();
        source_tasks.push(task::spawn(async move {
            src.run(tx, shutdown_rx).await;
        }));
        source_tasks.push(task::spawn(async move {
            while let Some(event) = rx.recv().await {
                fan_out(event, &downs).await;
            }
        }));
    }

    // Drop channels to ensure they are closed when all tasks finish
    drop(transform_channels);
    drop(sink_channels);
    drop(downstreams);

    for t in source_tasks {
        let _ = t.await;
    }
    for t in transform_tasks {
        let _ = t.await;
    }
    for t in sink_tasks {
        let _ = t.await;
    }

    Ok(())
}
