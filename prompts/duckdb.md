# DuckDB style CLI

We are making a duckdb style CLI from Apache 2 licensed code, with
nice display formatting, that can read parquet files and run the
ClickBench benchmarks.

## Example usage

(TODO find a publicly accessible dataset to read from)

```sql
yadb> select * from parquet_scan('data.parquet') limit 5;
```

## Example output

```
+----+-------+---------------------+
| id | name  | timestamp           |
+----+-------+---------------------+
| 1  | Alice | 2024-01-01 12:00:00 |
| 2  | Bob   | 2024-01-02 13:00:00 |
| 3  | Carol | 2024-01-03 14:00:00 |
| 4  | Dave  | 2024-01-04 15:00:00 |
| 5  | Eve   | 2024-01-05 16:00:00 |
+----+-------+---------------------+
```

## Features

1. Read Parquet files from local or remote systems
2. Support SQL queries (SELECT, WHERE, LIMIT)
3. Display results in a tabular format

## Additional potential options

1. Add metrics to the CLI to show query execution time and resource
   usage such as memory, CPU and network bandwidth usage

## Recipes

[base](../recipes/base.md), [repl](../future-recipes/repl.md),
[parquet](../future-recipes/parquet.md)
