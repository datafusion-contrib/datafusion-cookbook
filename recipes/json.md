# JSON

Read and query newline-delimited JSON (NDJSON) files. Built into
DataFusion — no extra dependencies.

## Dependencies

None (included in the [base](base.md) recipe).

## Code

```rust
use datafusion::prelude::*;

let ctx = SessionContext::new();
ctx.register_json("t", "data.json", NdJsonReadOptions::default())
    .await?;
let df = ctx.sql("SELECT * FROM t LIMIT 5").await?;
df.show().await?;
```

## Verify

TODO: query a small checked-in NDJSON file and check the output.

## Notes

- Only newline-delimited JSON is supported, not arbitrary nested JSON
  documents.
