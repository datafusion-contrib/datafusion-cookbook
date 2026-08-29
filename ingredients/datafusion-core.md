---
name: datafusion-core
title: DataFusion Core
category: base
summary: The query engine itself — SQL and DataFrame APIs, Arrow memory model, built-in Parquet/CSV/JSON/Avro readers.
when_to_use: Always. Every recipe starts here.
crate: datafusion
version: "54.1.0"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/apache/datafusion
install: cargo add datafusion@54.1.0 tokio@1 --features tokio/full
status: stable
pitfalls:
  - "DataFusion 55.0.0 is the latest release, but this cookbook pins 54.1.0. Almost every contrib crate still requires datafusion ^54, and 54/55 cannot coexist in one dependency graph. See the Version Compatibility section of the README before overriding this."
  - "Requires an async runtime. Add tokio with the `full` feature or `#[tokio::main]` will not resolve."
  - "The query engine is `SessionContext`. Creating one per query is fine but you lose registered tables and any configured object stores."
  - "There is no `parquet_scan()` / `read_parquet()` SQL function. DuckDB-style direct file queries need `SessionContext::new().enable_url_table()`, after which `SELECT * FROM 'file.parquet'` works. Without it that query fails with a table-not-found error."
  - "`SHOW TABLES` and `DESCRIBE` are off by default and fail with 'SHOW TABLES is not supported unless information_schema is enabled' — which reads like a missing feature rather than a config flag. Build the context from `SessionConfig::new().with_information_schema(true)`."
  - "`SHOW TABLES` also lists the information_schema views themselves. For a user-facing table list, query `information_schema.tables` and filter out `table_schema = 'information_schema'`."
example: https://github.com/apache/datafusion/tree/main/datafusion-examples
---

`SessionContext` is the entry point. Register data, then query it with SQL or
the DataFrame API.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a Parquet file as a table named `hits`
    ctx.register_parquet("hits", "data/hits.parquet", ParquetReadOptions::default())
        .await?;

    let df = ctx.sql("SELECT count(*) FROM hits").await?;
    df.show().await?;

    Ok(())
}
```

`df.show()` prints an Arrow-formatted table to stdout, which is already close to
the display format most CLI prompts ask for. See the
[Pretty Printing](pretty-printing.md) ingredient if you need the string rather
than stdout.

To query a file without registering it first, opt in to the dynamic file
catalog. This is the DataFusion equivalent of DuckDB's `parquet_scan()`:

```rust
let ctx = SessionContext::new().enable_url_table();
let df = ctx.sql("SELECT * FROM 'data/hits.parquet' LIMIT 5").await?;
```

Note the quotes around the path — it goes where a table name would.
