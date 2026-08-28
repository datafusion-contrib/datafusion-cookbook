---
name: tracing
title: OpenTelemetry Tracing
category: observability
summary: Wrap query execution in tracing spans, record native DataFusion metrics, and export to Jaeger, DataDog, or any OTLP collector.
when_to_use: You need to see where query time goes, or the prompt asks for metrics such as execution time and resource usage.
crate: datafusion-tracing
version: "54.0.0"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-tracing
install: cargo add datafusion-tracing@54.0.0
status: stable
pitfalls:
  - "The crate version tracks the DataFusion version exactly: use 54.0.0 with DataFusion 54, 55.0.0 with DataFusion 55. Mixing them will not resolve. The crate's README shows 55.0.0 because it tracks the latest core release."
  - "Installed as a physical optimizer rule on a SessionStateBuilder, so the context must be built with `SessionContext::new_with_state`. A plain `SessionContext::new()` is not instrumented and produces no spans."
  - "Emits to a `tracing` subscriber. Without one initialised, everything is silently discarded and it looks like the integration failed."
example: https://github.com/datafusion-contrib/datafusion-tracing/tree/main/examples
---

```rust
use datafusion::{execution::SessionStateBuilder, prelude::*};
use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};

// A tracing subscriber must already be initialised (see examples/otlp.rs).

let options = InstrumentationOptions::builder()
    .record_metrics(true)
    .preview_limit(5)
    .build();

let instrument_rule = instrument_with_info_spans!(options: options);

let session_state = SessionStateBuilder::new()
    .with_default_features()
    .with_physical_optimizer_rule(instrument_rule)
    .build();

let ctx = SessionContext::new_with_state(session_state);
```

`record_metrics(true)` captures DataFusion's own per-operator metrics — output
row counts and elapsed time — onto the spans, which is usually what a
"show query execution time" requirement actually needs.

For a quick answer without a collector, `EXPLAIN ANALYZE` already reports
per-operator metrics to the terminal and needs no dependency at all.
