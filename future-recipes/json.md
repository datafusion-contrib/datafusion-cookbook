---
name: json
category: file-formats
provides: Read newline-delimited JSON files (built in to DataFusion)
status: draft
status_note: not yet verified
arrow_major: any
crates: none
---

# JSON

Read and query newline-delimited JSON (NDJSON) files. Built into
DataFusion — no extra dependencies.

## Dependencies

None (included in the [base](../recipes/base.md) recipe).

## Versions

Adds no arrow-based crates; works with any DataFusion version — see the
version table in [base](../recipes/base.md).

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
