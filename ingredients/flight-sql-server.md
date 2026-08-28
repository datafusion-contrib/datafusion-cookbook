---
name: flight-sql-server
title: Flight SQL Server
category: wire-transport
summary: Serve DataFusion over Arrow Flight SQL, so JDBC/ODBC/ADBC clients can connect to it as a database.
when_to_use: You want BI tools or remote clients to query your engine over a network, rather than embedding it in a single process.
crate: datafusion-flight-sql-server
version: "0.4.18"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-flight-sql-server
install: cargo add datafusion-flight-sql-server@0.4.18
status: stable
pitfalls:
  - "Pins datafusion ^54 and arrow-flight ^58.3. Pairing it with DataFusion 55 (which needs arrow 59) will not resolve."
  - "Pulls in datafusion-federation as a hard dependency, so the federation optimizer is present whether or not you use it."
  - "Serves gRPC over HTTP/2. Behind a proxy that only speaks HTTP/1.1 the connection fails in a way that looks like an auth problem."
example: https://github.com/datafusion-contrib/datafusion-flight-sql-server#usage
---

The server wraps a `SessionContext` and exposes it on a socket:

```rust
use datafusion_flight_sql_server::service::FlightSqlService;
use datafusion::prelude::*;

let ctx = SessionContext::new();
ctx.register_parquet("hits", "data/hits.parquet", ParquetReadOptions::default()).await?;

FlightSqlService::new(ctx.state())
    .serve("0.0.0.0:50051".to_string())
    .await?;
```

Clients then connect with any Flight SQL driver — the ADBC Flight SQL driver,
the JDBC driver, or `adbc_core` from Rust. This is the fastest route to
"my engine is reachable as a database" because you write no protocol code.

For the client half, see [ADBC](adbc.md). If you need the PostgreSQL wire
protocol instead — so `psql` and existing Postgres drivers work unmodified —
see [PostgreSQL Wire Protocol](postgres-wire.md).
