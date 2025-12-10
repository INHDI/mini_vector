# mini_vector

`mini_vector` is a minimal Vector-like log pipeline written in Rust. It accepts events from stdin or an HTTP endpoint, applies transforms, routes based on `log_type`, and delivers to sinks such as console or HTTP.

## Current capabilities
- **Sources:** stdin, HTTP (Axum) that accepts JSON payloads.
- **Transforms:** add_field, contains_filter, JSON parse, regex parse (syslog-style), schema normalization, and a basic script/"mini VRL" helper.
- **Routing:** simple router based on a `field` → `sink` mapping.
- **Sinks:** console printer and HTTP sender.
- **Config:** YAML-based (see [`mini_vector.yml`](mini_vector.yml)).

## Run locally
```bash
cargo run --release -- mini_vector.yml
```
- Stdin source: type into the terminal; each line becomes an event under `message`.
- HTTP source: configure `sources.<id>.type = http`, then POST JSON to `http://<address><path>`.

## Sample config
The repo ships with a syslog-focused example:
```yaml
sources:
  stdin:
    type: stdin

transforms:
  parse_syslog:
    type: regex_parse
    field: message
    pattern: '^(?P<timestamp>\S+)\s+(?P<host>\S+)\s+(?P<program>[^\[:]+)(?:\[\d+\])?:\s+(?P<severity>\w+)?\s*(?P<message>.*)$'
    drop_on_error: false
    remove_source: false

  normalize:
    type: normalize_schema
    timestamp_field: timestamp
    host_field: host
    severity_field: severity
    program_field: program
    message_field: message
    default_log_type: linux_syslog

  enrich:
    type: script
    script: |
      .soc_tenant = "tenant01"
      .severity = upcase(.severity)

router:
  field: log_type
  routes:
    - equals: linux_syslog
      sink: console

sinks:
  console:
    type: console
```

## Roadmap toward Vector parity
See [`STANDARDIZATION.md`](STANDARDIZATION.md) for how to align this project with [Vector](https://github.com/vectordotdev/vector): target module layout, config shape (components with `inputs`), buffering/backpressure, and mini-VRL/remap direction.

## Notes for AI assistants (Cursor, Copilot, etc.)
- Prefer the module layout and terminology described in `STANDARDIZATION.md` when adding features.
- Keep new code in small, testable modules; avoid growing `src/main.rs` further.
- Validate config at startup and fail fast with clear errors.
- Do not add try/catch (or `std::panic::catch_unwind`) around imports; follow idiomatic Rust error handling.
