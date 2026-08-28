---
name: postgres-connector
title: PostgreSQL
category: connectors
summary: Query live PostgreSQL tables from DataFusion SQL, with joins and filters pushed down to the database.
when_to_use: Data lives in an existing PostgreSQL database and you want to query or join it without exporting to files first.
crate: datafusion-table-providers
version: "0.13.1"
features: [postgres]
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-table-providers
install: cargo add datafusion-table-providers@0.13.1 --features postgres
status: stable
pitfalls:
  - "Requires datafusion ^54. It is the main reason this cookbook pins DataFusion 54 rather than the newer 55."
  - "Connection parameters go through `to_secret_map`, not a connection URL string. Passing a `postgres://` DSN will not compile."
  - "Without datafusion-federation, joins between two PostgreSQL tables are executed by pulling both sides into DataFusion. Add federation to push the join into the database."
  - "The repo has been split into leaf crates such as `datafusion-table-providers-postgres`. The facade crate and its feature flags still work and are what this entry pins."
example: https://github.com/datafusion-contrib/datafusion-table-providers/blob/main/core/examples/postgres.rs
---

Build a pool, wrap it in a factory, and register the resulting `TableProvider`:

```rust
use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    postgres::PostgresTableFactory,
    sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    util::secrets::to_secret_map,
};
use std::{collections::HashMap, sync::Arc};

let params = to_secret_map(HashMap::from([
    ("host".to_string(), "localhost".to_string()),
    ("user".to_string(), "postgres".to_string()),
    ("db".to_string(), "postgres_db".to_string()),
    ("pass".to_string(), "password".to_string()),
    ("port".to_string(), "5432".to_string()),
    ("sslmode".to_string(), "disable".to_string()),
]));

let pool = Arc::new(PostgresConnectionPool::new(params).await?);
let factory = PostgresTableFactory::new(pool);
```

The same crate covers MySQL, SQLite, DuckDB, ClickHouse, MongoDB, ODBC, and
Flight SQL behind their own feature flags — the shape of the code is the same
for each, only the pool and factory types change.

Pair with [Query Federation](federation.md) so cross-table joins are pushed
into PostgreSQL instead of being executed locally.
