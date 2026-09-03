---
name: flight-sql-client
category: wire-transport
provides: Query a Flight SQL server from the command line
status: verified
verified: 2026-09-03
arrow_major: 59
crates: arrow-flight@59.2.0
datafusion: any
---

# Flight SQL client

Query any Flight SQL server with the `flight_sql_client` binary that ships
inside [arrow-flight](https://docs.rs/arrow-flight). No client code to write.

## Dependencies

```shell
cargo install arrow-flight@59.2.0 \
  --features cli,flight-sql,tls-ring \
  --bin flight_sql_client
```

All three features are required. Omitting any of them fails with:

```text
target `flight_sql_client` in package `arrow-flight` requires the features:
`cli`, `flight-sql`, `tls-ring`
```

Note that `flight-sql-experimental` is *not* the right feature name for this
binary despite appearing in the feature list.

## Versions

**The client does not need to match the server's arrow version.** Flight SQL
is a wire protocol: the client is a separate process, so it has its own
dependency graph. The arrow-major rule in [base](base.md) applies within one
binary, not across a client/server pair.

This matters here because the two cannot match with current releases:

| side | crate | arrow major |
|------|-------|------------:|
| server | `datafusion-flight-sql-server` 0.4.18 | 58 |
| client | `arrow-flight` `cli` feature | 59 |

`arrow-flight` 58.4.0 has no `cli` feature — it was added in 59.x — so the
built-in client cannot be compiled against arrow 58 at all. Verified working
against an arrow 58 server on 2026-09-03.

| arrow-flight | has `cli` binary |
|-------------:|------------------|
| 59.x         | yes              |
| 58.x         | no               |

## Code

None. The binary is the deliverable.

```console
$ flight_sql_client --host 127.0.0.1 --port 50051 \
    statement-query "select passenger_count, count(*) as trips from trips group by passenger_count order by passenger_count limit 3"
```

## Verify

Against a running [flight-sql-server](flight-sql-server.md):

```console
$ flight_sql_client --host 127.0.0.1 --port 50051 \
    statement-query "select count(*) as n from trips"
+---------+
| n       |
+---------+
| 2964624 |
+---------+
```

Expected: an aligned table with the correct row count. Timestamps, decimals
and aggregates all round-trip; verified against a 2.9M-row NYC taxi Parquet
file.

## Notes

- Use `--help` for the other subcommands (`get-catalogs`, `get-tables`,
  `get-db-schemas`, `prepared-statement-query`).
- Add `--headers key=value` for auth headers when the server requires them.
- Source:
  [flight_sql_client.rs](https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/bin/flight_sql_client.rs)
  is a good reference for writing a client in Rust rather than shelling out.
- To consume a Flight SQL server *as a table* inside DataFusion, use the
  `flight` feature of `datafusion-table-providers` instead of this binary.
