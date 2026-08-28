---
name: postgres-wire
title: PostgreSQL Wire Protocol
category: wire-transport
summary: Expose DataFusion over the PostgreSQL wire protocol so psql, DBeaver, Metabase, and Grafana connect without any Arrow-aware driver.
when_to_use: Clients already speak PostgreSQL and you do not want them to install a Flight SQL driver.
crate: datafusion-postgres
version: "0.18.0"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-postgres
install: cargo add datafusion-postgres@0.18.0
status: stable
pitfalls:
  - "Requires datafusion ^54."
  - "The CLI is a separate crate, `datafusion-postgres-cli`. Running `cargo install datafusion-postgres` does not give you the binary."
  - "Client tools that introspect `pg_catalog` on connect need `setup_pg_catalog` to be called explicitly. Skip it and psql connects but tab-completion and DBeaver's schema browser come back empty."
  - "DataFusion's SQL dialect is not PostgreSQL's. Queries lifted from a Postgres codebase can fail to plan even though the wire protocol works."
example: https://github.com/datafusion-contrib/datafusion-postgres#quick-start
---

As a library, `serve` takes an `Arc<SessionContext>` and options:

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::{serve, ServerOptions};
use datafusion_pg_catalog::setup_pg_catalog;

let session_context = Arc::new(SessionContext::new());
// register tables on session_context here

// Needed for psql/DBeaver metadata introspection
setup_pg_catalog(session_context.clone(), "datafusion")?;

let server_options = ServerOptions::new()
    .with_host("127.0.0.1".to_string())
    .with_port(5432);

serve(session_context, &server_options).await
```

The CLI serves files with no code at all, which is often enough:

```console
$ cargo install datafusion-postgres-cli
$ datafusion-postgres-cli --parquet hits:data/hits.parquet
$ psql -h 127.0.0.1 -p 5432 -U postgres -c "select count(*) from hits"
```

`--dir` registers every supported file in a directory as a table. Upstream
reports psql, DBeaver, pgcli, Metabase, and Grafana working; PowerBI and
DataGrip are listed as not yet working.
