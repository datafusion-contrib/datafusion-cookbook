---
name: arrow-flight
title: Arrow Flight
category: wire-transport
summary: The raw gRPC protocol for moving Arrow batches between processes, underneath Flight SQL.
when_to_use: You need a custom data-transfer protocol. If you want a SQL-queryable endpoint, take Flight SQL Server instead.
crate: arrow-flight
version: "58.4.0"
features: [flight-sql]
datafusion: "54"
arrow: "58"
license: Apache-2.0
repo: https://github.com/apache/arrow-rs
install: cargo add arrow-flight@58.4.0 --features flight-sql
status: stable
pitfalls:
  - "The arrow-flight major must match the arrow major DataFusion links. DataFusion 54 uses arrow 58.3, so use arrow-flight 58.x. DataFusion 55 uses arrow 59 and needs arrow-flight 59.x."
  - "Most people reaching for this want Flight SQL Server, which implements the query endpoints for you. Writing raw Flight means implementing DoGet/GetFlightInfo by hand."
  - "The SQL types live behind the `flight-sql` feature and are absent by default, which reads as a missing module."
example: https://github.com/apache/arrow-rs/tree/main/arrow-flight/examples
---

Use this directly only when you are building a bespoke protocol — streaming
intermediate results between nodes of a distributed engine, for example, where
the Flight SQL request/response shape does not fit.

For the ordinary "let clients run SQL against my engine" case, take
[Flight SQL Server](flight-sql-server.md); it depends on this crate and
implements the endpoints already.

`arrow-flight` follows arrow-rs versioning, so pair it with the arrow major
that DataFusion links:

| DataFusion | arrow | arrow-flight |
|------------|-------|--------------|
| 54.x       | 58.3  | 58.x         |
| 55.x       | 59.2  | 59.x         |
