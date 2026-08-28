---
name: adbc
title: ADBC Client
category: wire-transport
summary: Arrow Database Connectivity — a JDBC/ODBC-shaped client API that returns Arrow batches instead of rows.
when_to_use: You are writing the client half, connecting to a Flight SQL or other ADBC-speaking server without per-row conversion overhead.
crate: adbc_core
version: "0.24.0"
datafusion: any
license: Apache-2.0
repo: https://github.com/apache/arrow-adbc
install: cargo add adbc_core@0.24.0
status: stable
pitfalls:
  - "This is the client abstraction, not a server. Pair it with the Flight SQL Server ingredient, or with an existing ADBC-speaking database."
  - "Concrete drivers are separate crates or dynamically loaded shared libraries. Adding `adbc_core` alone gives you traits and no way to connect."
  - "Driver-manager based drivers need the native library present at runtime; a missing .so surfaces as a driver-load error rather than a connection error."
example: https://github.com/apache/arrow-adbc/tree/main/rust
---

ADBC is the Arrow-native counterpart to JDBC/ODBC: the same
driver/database/connection/statement shape, but results arrive as Arrow record
batches, so there is no row-by-row marshalling between the wire and your
analytics code.

The natural pairing in this cookbook is
[Flight SQL Server](flight-sql-server.md) on the serving side and the ADBC
Flight SQL driver on the client side. That combination gets you a queryable
network database where Arrow data never leaves its columnar form end to end.

DataFusion can also *consume* ADBC sources: `datafusion-table-providers` has an
`adbc` feature that exposes an ADBC-backed `TableProvider`, letting DataFusion
query anything with an ADBC driver.
