---
name: zarr
title: Zarr
category: file-formats
summary: Chunked, compressed N-dimensional array storage, the standard format in scientific and geospatial computing.
when_to_use: Working with array-shaped scientific data — climate, satellite, genomics — that is already stored as Zarr.
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/arrow-zarr
status: unpublished
pitfalls:
  - "Not published to crates.io. It must be a git dependency, which means no version pinning through Cargo's registry and no `cargo add <name>` shorthand."
  - "Zarr is N-dimensional; SQL tables are two-dimensional. Check how the crate flattens array dimensions into rows and columns before building on it."
  - "Verify which DataFusion version the current main branch targets before wiring it in — with no releases there is no version contract to rely on."
example: https://github.com/datafusion-contrib/arrow-zarr
---

The repository provides an `arrow-zarr` crate plus Python bindings, but nothing
is on crates.io, so take it as a git dependency:

```toml
[dependencies]
arrow-zarr = { git = "https://github.com/datafusion-contrib/arrow-zarr" }
```

Pin a `rev` rather than tracking a branch — without a published version, a
branch dependency means builds change under you with no signal.

Because there is no release contract here, confirm the DataFusion version on
current main before building on it; the `datafusion: "54"` above reflects this
cookbook's base, not a guarantee from the crate.
