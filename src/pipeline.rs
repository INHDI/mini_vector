use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;

use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tracing::{info, warn};
use metrics;

use crate::config::{
    FullConfig, SinkBufferConfig, SinkConfig, SourceConfig, TransformConfig,
};
use crate::event::{Event, EventEnvelope, Value};
use crate::queue::{self, EnqueueStatus, SinkReceiver, SinkSender};
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
        "file" => Ok(Box::new(FileSource::new(name.to_string(), cfg.clone()))),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeKind {
    Source,
    Transform,
    Sink,
}

#[derive(Clone, Debug)]
struct Edge {
    from: String,
    to: String,
}

struct Graph {
    nodes: HashMap<String, NodeKind>,
    edges: Vec<Edge>,
}

#[derive(Clone)]
enum EdgeSender {
    Channel(mpsc::Sender<EventEnvelope>),
    Sink(SinkSender),
}

#[derive(Clone)]
struct Downstream {
    name: String,
    sender: EdgeSender,
}

fn build_graph(config: &FullConfig) -> anyhow::Result<Graph> {
    let mut nodes = HashMap::new();
    for name in config.sources.keys() {
        nodes.insert(name.clone(), NodeKind::Source);
    }
    if let Some(transforms) = &config.transforms {
        for name in transforms.keys() {
            nodes.insert(name.clone(), NodeKind::Transform);
        }
    }
    for name in config.sinks.keys() {
        nodes.insert(name.clone(), NodeKind::Sink);
    }

    let mut edges: Vec<Edge> = Vec::new();

    if let Some(transforms) = &config.transforms {
        for (name, cfg) in transforms {
            for inp in &cfg.inputs {
                edges.push(Edge {
                    from: inp.clone(),
                    to: name.clone(),
                });
            }
            // route transform explicit outputs
            if cfg.kind == "route" {
                if let Some(routes) = &cfg.routes {
                    for (_rname, rcfg) in routes {
                        for out in &rcfg.outputs {
                            edges.push(Edge {
                                from: name.clone(),
                                to: out.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    for (name, cfg) in &config.sinks {
        if cfg.inputs.is_empty() {
            anyhow::bail!("sink '{}' requires at least one input", name);
        }
        for inp in &cfg.inputs {
            edges.push(Edge {
                from: inp.clone(),
                to: name.clone(),
            });
        }
    }

    // Validate nodes
    for edge in &edges {
        if !nodes.contains_key(&edge.from) {
            anyhow::bail!("edge from unknown node '{}'", edge.from);
        }
        if !nodes.contains_key(&edge.to) {
            anyhow::bail!("edge to unknown node '{}'", edge.to);
        }
    }

    // Cycle detection (Kahn)
    let mut incoming: HashMap<String, usize> = nodes.keys().map(|n| (n.clone(), 0)).collect();
    for e in &edges {
        *incoming.entry(e.to.clone()).or_default() += 1;
    }

    let mut queue: VecDeque<String> = incoming
        .iter()
        .filter_map(|(n, &deg)| if deg == 0 { Some(n.clone()) } else { None })
        .collect();
    let mut visited = 0;

    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for e in &edges {
        adj.entry(e.from.clone())
            .or_default()
            .push(e.to.clone());
    }

    while let Some(n) = queue.pop_front() {
        visited += 1;
        if let Some(neigh) = adj.get(&n) {
            for m in neigh {
                if let Some(entry) = incoming.get_mut(m) {
                    *entry -= 1;
                    if *entry == 0 {
                        queue.push_back(m.clone());
                    }
                }
            }
        }
    }

    if visited != nodes.len() {
        anyhow::bail!("cycle detected in pipeline graph");
    }

    Ok(Graph { nodes, edges })
}

async fn fan_out(event: EventEnvelope, downstreams: &[Downstream]) {
    let mut event = event;
    let mut target: Option<String> = None;
    let Event::Log(log) = &mut event.event;
    if let Some(Value::String(route)) = log.fields.remove("__route_target") {
        target = Some(route);
    }

    for ds in downstreams {
        if let Some(ref t) = target {
            if ds.name != *t {
                continue;
            }
        }
        match &ds.sender {
            EdgeSender::Channel(tx) => {
                let cloned = event.clone();
                if let Err(err) = tx.send(cloned).await {
                    warn!("downstream send failed: {}", err);
                    event.ack.ack();
                }
            }
            EdgeSender::Sink(sender) => {
                let cloned = event.clone();
                if matches!(sender.send(cloned.clone()).await, EnqueueStatus::Dropped) {
                    metrics::increment_counter!(
                        "events_dropped",
                        "component" => ds.name.clone(),
                        "reason" => "buffer_full"
                    );
                    cloned.ack.ack();
                }
            }
        }
    }
    event.ack.ack();
}

fn merge_inputs(inputs: Vec<mpsc::Receiver<EventEnvelope>>) -> mpsc::Receiver<EventEnvelope> {
    let (tx, rx) = mpsc::channel::<EventEnvelope>(DEFAULT_CHANNEL_SIZE);
    for mut r in inputs {
        let tx_clone = tx.clone();
        task::spawn(async move {
            while let Some(ev) = r.recv().await {
                if tx_clone.send(ev).await.is_err() {
                    break;
                }
            }
        });
    }
    rx
}

pub async fn run_pipeline(
    config: FullConfig,
    external_shutdown: Option<broadcast::Receiver<()>>,
    health: crate::health::HealthState,
) -> anyhow::Result<()> {
    let graph = build_graph(&config)?;

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

    // Build sink queues
    let mut sink_channels: HashMap<String, SinkSender> = HashMap::new();
    let mut sink_receivers: HashMap<String, SinkReceiver> = HashMap::new();
    for (name, scfg) in &config.sinks {
        let buffer: SinkBufferConfig = scfg
            .buffer
            .clone()
            .unwrap_or_else(|| SinkBufferConfig {
                max_events: DEFAULT_CHANNEL_SIZE,
                ..SinkBufferConfig::default()
            });
        let queue_path = buffer.queue_path.clone().map(|p| std::path::PathBuf::from(p));
        let max_segment_bytes = if buffer.max_bytes > 0 {
            buffer.max_bytes as u64
        } else {
            64 * 1024 * 1024
        };
        let (tx, rx) = queue::sink_queue(
            name.clone(),
            buffer.max_events,
            buffer.when_full,
            buffer.queue,
            queue_path,
            max_segment_bytes,
        )?;
        sink_channels.insert(name.clone(), tx);
        sink_receivers.insert(name.clone(), rx);
    }

    // Channels per edge
    let mut edge_senders: HashMap<(String, String), EdgeSender> = HashMap::new();
    let mut input_receivers: HashMap<String, Vec<mpsc::Receiver<EventEnvelope>>> = HashMap::new();
    let mut downstreams: HashMap<String, Vec<Downstream>> = HashMap::new();

    for e in &graph.edges {
        let to_kind = graph
            .nodes
            .get(&e.to)
            .copied()
            .unwrap_or(NodeKind::Transform);
        match to_kind {
            NodeKind::Sink => {
                if let Some(sender) = sink_channels.get(&e.to) {
                    edge_senders.insert(
                        (e.from.clone(), e.to.clone()),
                        EdgeSender::Sink(sender.clone()),
                    );
                } else {
                    warn!("missing sink sender for {}", e.to);
                }
            }
            _ => {
                let (tx, rx) = mpsc::channel::<EventEnvelope>(DEFAULT_CHANNEL_SIZE);
                edge_senders.insert(
                    (e.from.clone(), e.to.clone()),
                    EdgeSender::Channel(tx.clone()),
                );
                input_receivers.entry(e.to.clone()).or_default().push(rx);
            }
        }
    }

    for e in &graph.edges {
        if let Some(sender) = edge_senders.get(&(e.from.clone(), e.to.clone())) {
            downstreams
                .entry(e.from.clone())
                .or_default()
                .push(Downstream {
                    name: e.to.clone(),
                    sender: sender.clone(),
                });
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
            if let Some(mut inputs) = input_receivers.remove(name) {
                let downs = downstreams.get(name).cloned().unwrap_or_default();
                let merged_rx = if inputs.len() == 1 {
                    inputs.remove(0)
                } else {
                    merge_inputs(inputs)
                };

                let (tx_out, mut rx_out) = mpsc::channel::<EventEnvelope>(DEFAULT_CHANNEL_SIZE);

                transform_tasks.push(task::spawn(async move {
                    t.run(merged_rx, tx_out).await;
                }));

                transform_tasks.push(task::spawn(async move {
                    while let Some(event) = rx_out.recv().await {
                        fan_out(event, &downs).await;
                    }
                }));
            } else {
                warn!("transform '{}' has no inputs connected", name);
            }
        }
    }

    // Spawn source tasks + fanout
    let mut source_tasks = Vec::new();
    for (name, scfg) in &config.sources {
        let src = build_source(name, scfg)?;
        let downs = downstreams.get(name).cloned().unwrap_or_default();
        let (tx, mut rx) = mpsc::channel::<EventEnvelope>(DEFAULT_CHANNEL_SIZE);
        
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
