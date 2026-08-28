---
name: variant
title: Variant
category: file-formats
summary: UDFs for the open Variant binary type — semi-structured data with a compact binary encoding rather than reparsed text.
when_to_use: You have semi-structured data heavy enough that repeatedly parsing JSON text is the bottleneck.
crate: datafusion-variant
version: "0.1.0"
datafusion: "52"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-variant
install: cargo add datafusion-variant@0.1.0
status: experimental
pitfalls:
  - "Requires datafusion ^52.1 and arrow ^57 — two majors behind this cookbook's DataFusion 54 base. It cannot be combined with the rest of the menu as published. Treat it as a preview and check for a newer release before planning around it."
  - "At 0.1.0 with no releases since, the API should be expected to change."
  - "Variant support is moving into Arrow and DataFusion upstream. Check whether core already covers your case before adding this."
example: https://github.com/datafusion-contrib/datafusion-variant/tree/main/examples
---

Variant is an open binary encoding for semi-structured data — the same idea as
Spark's and Snowflake's variant types — giving typed field access without
reparsing text on every query.

**Compatibility warning.** This is the one ingredient on the menu that does not
compose with the others right now. It pins DataFusion 52 while everything else
here needs 54, and Cargo cannot satisfy both. Until it is updated, use
[JSON Functions](json-functions.md) for semi-structured data instead — it
tracks DataFusion releases closely and works with the rest of the menu today.

Also see `datafusion-contrib/datafusion-functions-variant`, a separate effort
toward the same goal, and the Variant work landing in Arrow and DataFusion
core.
