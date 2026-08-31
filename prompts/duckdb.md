---
name: duckdb
builds: DuckDB style interactive SQL CLI
recipes: [base, repl, parquet]
---

# DuckDB style CLI

We are making a duckdb style CLI from Apache 2 licensed code, with
nice display formatting, that can read parquet files and run the
ClickBench benchmarks.

## Example usage

Here is how to run the CLI and query a remote Parquet file (the ClickBench `hits.parquet` file):

```sql
yadb> select count(*) from parquet_scan('https://datasets.clickhouse.com/hits_compatible/hits.parquet') limit 5;
```

## Example output

```
+----------+
| count(*) |
+----------+
| 100000000  |
+----------+
```

## Features

1. Read Parquet files from local or remote systems
2. Support SQL queries (SELECT, WHERE, LIMIT)
3. Display results in a tabular format
4. Support vortex files (using the vortex recipe)

## Additional potential options

1. Add metrics to the CLI to show query execution time and resource
   usage such as memory, CPU and network bandwidth usage
