# Parquet

Read and query Apache Parquet files. Built into DataFusion — no extra
dependencies.

## Dependencies

None (included in the [base](../recipes/base.md) recipe).

## Versions

Adds no arrow-based crates; works with any DataFusion version — see the
version table in [base](../recipes/base.md).

## Code

```rust
use datafusion::prelude::*;

let ctx = SessionContext::new();
ctx.register_parquet("t", "data.parquet", ParquetReadOptions::default())
    .await?;
let df = ctx.sql("SELECT * FROM t LIMIT 5").await?;
df.show().await?;
```

## Verify

TODO: query a known public Parquet file (e.g. ClickBench
`hits.parquet`) and check the output row count.

## Notes

- SQL alternative: `SELECT * FROM 'data.parquet'` works without
  registering a table.
