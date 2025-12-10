# Checklist Feasibility

This note reviews the proposed checklist against the current `mini_vector` implementation and Vector's model.

## Overall assessment

* The checklist is feasible: it builds on existing components (HTTP/stdin sources, transforms, router, sinks) without replacing them wholesale, and each item can be phased in incrementally.
* Risk/effort increases when introducing a DAG topology and a VRL-like remap language; these are larger refactors but still achievable with staged rollouts.

## Why it fits the current codebase

* Sources, transforms, and sinks are already trait-based (`Source`, `Transform`, `Sink`), so adding configs and new variants follows the existing factory pattern in `build_source`, `build_transform`, and `build_sink`.
* The parser-oriented transforms (`JsonParseTransform`, `RegexParseTransform`, `NormalizeSchemaTransform`) already expose error-handling knobs (`drop_on_error`, `remove_source`), which aligns with the checklist items to standardize parse behavior.
* Routing is currently centralized (`RouterConfig` + `RouterTransform`), making it straightforward to refactor into an input-based DAG and route transform.

## Recommended implementation order

1) **Harden existing parse/normalize transforms**: add `keep_original/target_prefix/flag` options and solidify schema defaults so downstream changes have a stable contract.
2) **Introduce DAG topology with `inputs`**: extend configs with `inputs`, build a component graph, and migrate the router into a `route` transform. This unlocks per-edge buffering later.
3) **Add sink buffering/backpressure config**: reuse channel sizing to expose `buffer.max_events` and `when_full` modes, preparing for future disk buffering.
4) **Prototype mini-VRL/remap**: start small (assignment, conditionals, helpers) to replace bespoke transforms over time; keep compile-time validation for scripts.
5) **Layer in observability (metrics/logging)**: attach counters to components and expose a Prometheus endpoint; useful for validating earlier steps and monitoring backpressure.

## Feasibility risks and mitigations

* **Graph rewrite complexity**: keep a compatibility shim (old router path) while introducing `inputs` to avoid a big-bang refactor.
* **Mini-VRL scope creep**: begin with a narrow feature set focused on SOC pipelines; consider embedding Vector's VRL crate if maintenance cost grows.
* **Buffering correctness**: start with in-memory bounded channels and clear drop/block semantics before attempting disk/WAL buffering.

Overall, the checklist is realistic for the current codebase when tackled in the order above.
