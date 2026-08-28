---
name: json-functions
title: JSON Functions
category: file-formats
summary: SQL functions for querying JSON held in string columns — `json_get`, `json_get_str`, `json_contains`, and the `->` operator.
when_to_use: JSON is stored as text inside a column and its shape varies per row, so it cannot be modelled as a fixed Arrow struct.
crate: datafusion-functions-json
version: "0.54.2"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-functions-json
install: cargo add datafusion-functions-json@0.54.2
status: stable
pitfalls:
  - "The crate's major.minor tracks the DataFusion version it targets: 0.54.x is for DataFusion 54.x. Picking the newest release without matching that pairing is the usual cause of build failure."
  - "Functions must be registered explicitly with `register_all`. They are not picked up merely by adding the dependency, and the failure looks like an unrelated `Invalid function` planning error."
example: https://github.com/datafusion-contrib/datafusion-functions-json#usage
---

Register the functions into the context before planning any query that uses
them:

```rust
let mut ctx = SessionContext::new();
datafusion_functions_json::register_all(&mut ctx)?;
```

Then the JSON accessors are available in SQL:

```sql
SELECT
  json_get_str(payload, 'user', 'name')  AS user_name,
  json_get_int(payload, 'retries')       AS retries
FROM events
WHERE json_contains(payload, 'error')
```

`register_all` takes `&mut SessionContext`, so bind the context mutably. The
accessors return NULL rather than erroring when a path is absent, which makes
them safe over heterogeneous rows.
