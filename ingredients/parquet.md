---
name: parquet
title: Parquet
category: file-formats
summary: Read and write Parquet, with predicate and projection pushdown, row-group pruning, and page-index filtering.
when_to_use: The default columnar format. Assume Parquet unless the prompt says otherwise.
datafusion: "54"
license: Apache-2.0
repo: https://github.com/apache/arrow-rs
status: stable
pitfalls:
  - "Enabled by DataFusion's default features. Do not add the `parquet` crate separately unless you are working with the low-level reader — mismatched arrow majors are the usual result."
  - "Registering a directory reads every Parquet file in it as one table, and fails if their schemas differ. Pass an explicit schema, or use `ListingOptions` with `schema_infer_max_records`, when files have drifted."
  - "Pruning statistics only help if the file was written with them. Files produced by some writers have no row-group statistics, and every query becomes a full scan regardless of the predicate."
example: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/data_io
---

Registered as a named table:

```rust
ctx.register_parquet("hits", "data/hits.parquet", ParquetReadOptions::default()).await?;
```

A whole directory as a single partitioned table:

```rust
ctx.register_parquet("hits", "data/hits/", ParquetReadOptions::default()).await?;
```

Or queried directly by path, with [`enable_url_table`](datafusion-core.md).
Writing results back out:

```rust
df.write_parquet("out/", DataFrameWriteOptions::new(), None).await?;
```

`write_parquet` takes a *directory* and writes one file per partition. To get a
single file, call `.repartition(Partitioning::RoundRobinBatch(1))` first, or
use the lower-level `ArrowWriter`.
