---
name: federation
title: Query Federation
category: connectors
summary: Push joins, filters, and TopK queries down into the remote engine that owns the data instead of pulling rows into DataFusion.
when_to_use: You have registered two or more tables from the same remote database and want the join executed there rather than locally.
crate: datafusion-federation
version: "0.5.5"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-federation
install: cargo add datafusion-federation@0.5.5
status: stable
pitfalls:
  - "You must build the SessionContext from the federated session state. Adding the dependency alone changes nothing, and the symptom is silently slow queries rather than an error."
  - "Only pushes down where the remote engine supports the operation. A DataFusion-specific UDF in the predicate blocks pushdown for the whole subtree."
  - "Requires datafusion ^54, matching the table providers."
example: https://github.com/datafusion-contrib/datafusion-federation/tree/main/examples
---

Federation is enabled by constructing the context from its session state:

```rust
use datafusion::prelude::SessionContext;

let state = datafusion_federation::default_session_state();
let ctx = SessionContext::new_with_state(state);

// Register remote table providers into ctx as usual;
// queries across them are now federated.
```

Confirm it is working with `EXPLAIN`. A federated plan shows a single remote
scan node containing the pushed-down SQL, rather than two scans feeding a
local `HashJoinExec`:

```sql
EXPLAIN SELECT c.name, o.total
FROM companies c JOIN orders o ON o.company_id = c.id;
```

If you still see two separate scans and a local join, something in the query
blocked pushdown — most often a function the remote engine does not have.
