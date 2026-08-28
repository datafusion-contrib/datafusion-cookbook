---
name: json
title: JSON (newline-delimited)
category: file-formats
summary: Read newline-delimited JSON (NDJSON) files with inferred schemas.
when_to_use: Log-style data, one JSON object per line. Built in — no extra dependency.
datafusion: "54"
license: Apache-2.0
repo: https://github.com/apache/arrow-rs
status: stable
pitfalls:
  - "This reads newline-delimited JSON, not a JSON array. A file containing `[{...},{...}]` will not parse. Convert to one object per line first."
  - "Schema inference samples a bounded number of records. A field that only appears late in a large file will be missing from the schema, and queries against it fail with a column-not-found error. Raise `schema_infer_max_rec` or supply the schema explicitly."
  - "Nested objects become Arrow struct columns, addressed with dot notation. Genuinely dynamic JSON is better served by the JSON Functions ingredient."
example: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionContext.html#method.register_json
---

```rust
use datafusion::prelude::*;

ctx.register_json("logs", "data/logs.ndjson", NdJsonReadOptions::default()).await?;
let df = ctx.sql("SELECT level, count(*) FROM logs GROUP BY level").await?;
```

Widening the inference window, and pinning the extension when files are not
named `.json`:

```rust
let options = NdJsonReadOptions::default()
    .file_extension(".ndjson");

ctx.register_json("logs", "data/logs/", options).await?;
```

Nested fields are struct columns:

```sql
SELECT request.method, request.path FROM logs WHERE request.status >= 500
```
