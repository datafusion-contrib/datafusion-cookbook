# Vortex CLI

We are making an interactive SQL command line tool like
[`datafusion-cli`] that can also read and write Vortex format files.

[`datafusion-cli`]: https://datafusion.apache.org/user-guide/cli/

## Example usage

```shell
$ vortex-cli
vortex-cli> select count(*) from 'hits.vortex';
vortex-cli> copy (select * from 'data.parquet') to 'data.vortex';
vortex-cli> select * from 'data.vortex' limit 5;
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
5 rows in set. Query took 0.012 seconds.
```

## Features

1. Interactive SQL REPL with tabular output and query timing, like
   `datafusion-cli`
2. Read the file formats built in to DataFusion (Parquet, CSV, JSON)
3. Read Vortex files by querying them directly by path
4. Write Vortex files with `COPY ... TO ...`

## Additional potential options

1. `CREATE EXTERNAL TABLE ... STORED AS vortex`
2. Read files from object storage (S3, GCS, ...) as well as the local
   filesystem
3. Convert between formats (e.g. Parquet ↔ Vortex) and compare file
   sizes and query times

## Recipes

[base](../recipes/base.md), [repl](../future-recipes/repl.md),
[parquet](../future-recipes/parquet.md), [vortex](../recipes/vortex.md)
