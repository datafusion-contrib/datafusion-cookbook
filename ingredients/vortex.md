---
name: vortex
title: Vortex
category: file-formats
summary: A next-generation compressed columnar format with random access and pushdown, positioned as a faster alternative to Parquet.
when_to_use: Scan performance on Parquet is the bottleneck and you control both writer and reader.
crate: vortex-datafusion
version: "0.85.0"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/spiraldb/vortex
install: cargo add vortex-datafusion@0.85.0
status: experimental
pitfalls:
  - "Depends on the fine-grained datafusion-* subcrates at ^54, not the datafusion facade. It composes with DataFusion 54 but the dependency tree looks different from other ingredients."
  - "Moves fast and its version is nowhere near DataFusion's. Pin an exact version; 0.x minor bumps carry breaking changes."
  - "Not an interchange format. Nothing outside the Vortex ecosystem reads these files, so keep Parquet if anything else consumes the output."
example: https://github.com/spiraldb/vortex
---

Vortex is a columnar file format built around cascading compression schemes
that stay queryable while compressed, so filters and projections can run
against encoded data rather than requiring a decode first.

Take it when profiling shows Parquet scan time dominating and you own both
writer and reader. Keep [Parquet](parquet.md) when the files cross a system
boundary.
