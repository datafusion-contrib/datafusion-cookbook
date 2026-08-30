# Prompts

This file contains example prompts for interesting systems you may wish to try
out.

Each prompt has **acceptance criteria**: the concrete things the built system
must do. [TESTING.md](TESTING.md) scores runs against them.

## Test data

Publicly accessible, no credentials, verified reachable:

| Dataset | Size | URL |
|---------|------|-----|
| NYC taxi, one month | 48 MB | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` |
| ClickBench, one partition | 117 MB | `https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet` |
| ClickBench, full | 14 GB | `https://datasets.clickhouse.com/hits_compatible/hits.parquet` |

Start with the NYC taxi file. It is small enough to download in seconds and
has a mix of types — timestamps, decimals, integers, strings — that exercises
display formatting.

For generated multi-table data instead, see the
[TPC-H ingredient](ingredients/tpch.md).

---

## "DuckDB style CLI"

> Make a DuckDB style CLI using Apache 2 licensed code, with nice display
> formatting, that can read Parquet and JSON files.

### Example usage

```console
$ yadb
yadb> select tpep_pickup_datetime, passenger_count, total_amount
      from 'yellow_tripdata_2024-01.parquet'
      limit 5;
```

### Example output

```text
+----------------------+-----------------+--------------+
| tpep_pickup_datetime | passenger_count | total_amount |
+----------------------+-----------------+--------------+
| 2024-01-01T00:57:55  | 1               | 22.7         |
| 2024-01-01T00:03:00  | 1               | 18.75        |
| 2024-01-01T00:17:06  | 1               | 31.3         |
| 2024-01-01T00:36:38  | 1               | 17.0         |
| 2024-01-01T00:46:51  | 1               | 16.1         |
+----------------------+-----------------+--------------+
```

### Acceptance criteria

1. Reads Parquet files from a local path.
2. Reads newline-delimited JSON files from a local path.
3. Accepts SQL with `SELECT`, `WHERE`, `LIMIT`, `GROUP BY`, and aggregates.
4. Prints results as an aligned ASCII table.
5. Is an interactive REPL: arrow-key history, multi-line statements terminated
   by `;`, Ctrl-D to exit, Ctrl-C to abandon the current line.
6. Every dependency is under a permissive licence, and any non-Apache-2.0 one
   is called out explicitly.
7. Reports an error and returns to the prompt on bad SQL, rather than exiting.

### Worked recipe

[`recipes/duckdb-style-cli`](recipes/duckdb-style-cli/) is a build of this
prompt that meets all seven criteria, in ~250 lines and three dependencies.

### Notes for whoever runs this

Two things reliably go wrong, both recorded as pitfalls in the ingredients:

- **`parquet_scan()` does not exist in DataFusion.** Querying a file by path
  requires `SessionContext::new().enable_url_table()`, after which
  `SELECT * FROM 'file.parquet'` works. Agents that know DuckDB tend to write
  `parquet_scan('f.parquet')` and then debug a table-not-found error.
- **A second table-formatting crate gets added** — `comfy-table` or
  `prettytable-rs` — when DataFusion already re-exports Arrow's formatter.

Note also that [`datafusion-cli`](ingredients/datafusion-cli.md) already
satisfies most of this prompt, so record whether the run reuses it, reads its
source, or starts from an empty `main`.

### Optional extensions

1. Report query execution time and rows returned after each statement.
2. Add `EXPLAIN` / `EXPLAIN ANALYZE` passthrough.
3. Read Parquet directly from an HTTP URL or S3, not just a local path.
4. Run the ClickBench queries and print a timing table.

---

## "Postgres-compatible analytics endpoint"

> Serve a directory of Parquet files so that psql and Grafana can query them as
> if they were a PostgreSQL database.

### Acceptance criteria

1. `psql -h 127.0.0.1 -p 5432` connects successfully.
2. `SELECT` with joins and aggregates across at least two files works.
3. Tables are discoverable — `\dt` in psql lists them.
4. A second client (Grafana, DBeaver, or pgcli) connects and runs a query.

### Notes for whoever runs this

The relevant ingredient is
[PostgreSQL Wire Protocol](ingredients/postgres-wire.md). Criterion 3 is the
one that catches people: plain queries work without `setup_pg_catalog`, but
`\dt` returns nothing until it has been called.

---
