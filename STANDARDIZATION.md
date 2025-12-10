# Standardization guide (Vector-aligned)

This guide describes how to reshape `mini_vector` so it follows Vector's component model and project layout. Use it as a checklist when refactoring or when instructing AI assistants (Cursor, Copilot, etc.).

## Target module layout
Move away from a single `main.rs` toward small modules that mirror Vector's terminology:

```
src/
  main.rs            # binary entrypoint (parse args, load config, start runtime)
  config.rs          # config structs + validation (serde with `deny_unknown_fields`)
  pipeline.rs        # graph/DAG builder, topological sort, channel wiring
  components/
    source/
      mod.rs
      http.rs
      stdin.rs
    transform/
      mod.rs
      json_parse.rs
      regex_parse.rs
      normalize_schema.rs
      script.rs
      route.rs
    sink/
      mod.rs
      console.rs
      http.rs
  buffer.rs          # buffer config + in-memory bounded channels (backpressure)
  metrics.rs         # internal counters + Prometheus exporter
```

Keep traits (`Source`, `Transform`, `Sink`) in the component modules, not in `main.rs`. Add unit tests per module.

## Config shape (Vector-style)
Adopt Vector's component-with-inputs model and validate strictly:

```yaml
sources:
  in_http:
    type: http
    address: "0.0.0.0:9000"
    path: "/ingest"

transforms:
  parse_syslog:
    type: regex_parse
    inputs: ["in_http"]
    field: message
    pattern: "..."
    drop_on_error: false
    remove_source: false

  normalize:
    type: normalize_schema
    inputs: ["parse_syslog"]
    timestamp_field: timestamp
    host_field: host
    severity_field: severity
    program_field: program
    message_field: message
    default_log_type: linux_syslog

  route:
    type: route
    inputs: ["normalize"]
    routes:
      linux:
        condition: '.log_type == "linux_syslog"'
        outputs: ["console"]

sinks:
  console:
    type: console
    inputs: ["route"]
    buffer:
      max_events: 1024
      when_full: block   # or drop_newest
```

Guidelines:
- Use `#[serde(deny_unknown_fields)]` to fail fast on typos.
- Validate references: every `inputs` entry must exist, and the graph must be acyclic (topological sort).
- Keep defaults centralized (e.g., HTTP source path `/ingest`, sink buffer defaults).

## Runtime model
- Build a DAG of components (`Node { id, kind, inputs, outputs }`).
- Create an MPSC channel per edge; size derives from sink buffer config.
- Each component runs as a Tokio task: sources only send, transforms receive and forward, sinks only receive.
- Route transform replaces the global router; conditions can initially be string equality and later use mini-VRL.

## Backpressure & buffering
- Expose `buffer.max_events` and `buffer.when_full` for sinks; map directly to channel capacity and send mode (`send` vs `try_send` + drop counter).
- Keep the API ready for future disk/WAL buffers by introducing a `Buffer` trait with `MemoryBuffer` as the first implementation.

## Transform behavior conventions
Standardize parse/normalize behavior so pipelines behave predictably (matches Vector UX):
- Parsing transforms (`json_parse`, `regex_parse`, later `parse_syslog`) accept:
  - `drop_on_error` (bool): drop the event if parsing fails.
  - `keep_original`/`remove_source`: keep, move, or drop the source field after parse.
  - `target_prefix`: write parsed fields under a prefix instead of root.
- Normalize schema should guarantee presence of `@timestamp`, `host`, `severity`, `program`, `message`, and `log_type`, defaulting `@timestamp` to `now()` if missing.
- Script/mini-VRL should compile at startup; fail config load on syntax errors.

## Observability
- Attach counters per component: `events_in`, `events_out`, `events_dropped`, `errors`.
- Expose Prometheus metrics via a small Axum server (e.g., `127.0.0.1:9090`).
- Provide a CLI subcommand to print the graph (DOT/Graphviz) for quick inspection.

## Contributor workflow (human or AI)
- Start with `cargo fmt`, `cargo clippy`, and `cargo test` before submitting changes.
- Prefer incremental refactors: extract modules first, then add features.
- Update docs and sample configs alongside code changes.
- Keep commits scoped (one feature or refactor per commit) to ease review.
