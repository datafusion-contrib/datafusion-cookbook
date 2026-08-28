---
name: pretty-printing
title: Table-Formatted Output
category: repl
summary: Render Arrow RecordBatches as an aligned ASCII table, the `+----+-------+` format familiar from DuckDB and psql.
when_to_use: Any time query results go to a terminal. Nothing extra to install — it ships with DataFusion.
datafusion: "54"
license: Apache-2.0
repo: https://github.com/apache/arrow-rs
status: stable
pitfalls:
  - "Do not add `comfy-table` or `prettytable-rs` for this. DataFusion already re-exports Arrow's formatter, and adding a second table library is the single most common piece of redundant work in CLI builds."
  - "`df.show()` prints to stdout and returns `()`. When you need the text — for tests, or to page it — use `pretty_format_batches` instead."
  - "Formatting requires the full result set in memory, since column widths depend on every row. Apply a LIMIT before formatting interactive output."
  - "Use the `datafusion::arrow` re-export rather than adding `arrow` to Cargo.toml. Adding it directly risks pulling a different arrow major than DataFusion links, which produces `expected RecordBatch, found RecordBatch` type errors."
example: https://docs.rs/datafusion/latest/datafusion/arrow/util/pretty/index.html
---

The direct route, when you just want it on screen:

```rust
let df = ctx.sql("SELECT * FROM 'data/hits.parquet' LIMIT 5").await?;
df.show().await?;
```

When you need the formatted text as a `String` — for a pager, a test
assertion, or to write elsewhere:

```rust
use datafusion::arrow::util::pretty::pretty_format_batches;

let batches = ctx.sql(&sql).await?.collect().await?;
println!("{}", pretty_format_batches(&batches)?);
```

Both produce:

```text
+----+-------+---------------------+
| id | name  | timestamp           |
+----+-------+---------------------+
| 1  | Alice | 2024-01-01T12:00:00 |
| 2  | Bob   | 2024-01-02T13:00:00 |
+----+-------+---------------------+
```

`pretty_format_batches` takes `&[RecordBatch]`, so an empty result set
formats to an empty table rather than erroring — check `batches.is_empty()`
if you want to print something friendlier.
