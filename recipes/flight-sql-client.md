---
name: flight-sql-client
category: wire-transport
provides: Query a Flight SQL server from the command line
status: verified
verified: 2026-09-28
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

Any `arrow-flight` release from 56 on has the `cli`, `flight-sql` and
`tls-ring` features this install needs; 59.2.0 is the pinned, tested one.

## Code

None. The binary is the deliverable.

```console
$ flight_sql_client --host 127.0.0.1 --port 50051 \
    statement-query "select passenger_count, count(*) as trips from trips group by passenger_count order by passenger_count limit 3"
```

## Verify

Start a [flight-sql-server](flight-sql-server.md) serving the cookbook's
[fixture](../testing/data/README.md), then:

```console
$ flight_sql_client --host 127.0.0.1 --port 50051 \
    statement-query "select count(*) as n from trips"
+---+
| n |
+---+
| 6 |
+---+
$ flight_sql_client --host 127.0.0.1 --port 50051 tables datafusion
+--------------+----------------+------------+------------+
| catalog_name | db_schema_name | table_name | table_type |
+--------------+----------------+------------+------------+
| datafusion   | public         | trips      | Base       |
+--------------+----------------+------------+------------+
```

Expected: exactly the tables above.

## Notes

- Metadata subcommands: `catalogs`, `db-schemas`, `tables <CATALOG>`,
  `table-types`, plus `prepared-statement-query`. Note they are not prefixed
  with `get-`, despite the underlying Flight SQL RPCs being named `GetCatalogs` and so on.
- Add `-H key=value` (or `--header`, singular, repeatable) for auth headers
  when the server requires them.
- Source:
  [flight_sql_client.rs](https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/bin/flight_sql_client.rs)
  is a good reference for writing a client in Rust rather than shelling out.
- To consume a Flight SQL server *as a table* inside DataFusion, use the
  `flight` feature of `datafusion-table-providers` instead of this binary.
