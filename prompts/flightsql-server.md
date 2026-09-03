---
name: flightsql-server
builds: Flight SQL server exposing a directory of Parquet files, queried from the command line
recipes: [base, cli, flight-sql-server, flight-sql-client]
---

# Serve a directory of Parquet files over Flight SQL

We are making a server that exposes a directory of Parquet files as SQL
tables over Arrow Flight SQL, so that any Flight SQL client can query them
over the network without the files being copied.

Each `.parquet` file in the directory becomes one table, named after the file
stem. The server should report what it registered and keep running until it
is killed.

For querying it, use an existing Flight SQL command line client rather than
writing one.

## Example usage

Start the server:

```shell
flight-server ./data --addr 127.0.0.1:50051
```

Query it from another terminal:

```shell
flight_sql_client --host 127.0.0.1 --port 50051 \
  statement-query "select passenger_count, count(*) as trips from trips group by passenger_count"
```

## Example output

Server:

```text
registered trips
serving 1 table(s) on 127.0.0.1:50051
```

Client:

```text
+-----------------+---------+
| passenger_count | trips   |
+-----------------+---------+
| 0               | 31465   |
| 1               | 2188739 |
| 2               | 405103  |
+-----------------+---------+
```

## Features

1. Register every `.parquet` file in a directory as a table named after the
   file stem
2. Serve those tables over Arrow Flight SQL on a configurable host and port
3. Answer SQL with filters, aggregates and `GROUP BY`
4. Report the registered tables and the listen address on startup
5. Exit with a clear error when the directory holds no Parquet files

## Additional potential options

1. Serve a Flight SQL client as a second binary in the same project, instead
   of using the prebuilt one
2. Add authentication headers and TLS
3. Support `get-tables` / `get-db-schemas` metadata calls from the client
4. Watch the directory and register files added while running
5. Serve other formats (CSV, NDJSON) alongside Parquet
