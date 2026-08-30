This file contains example prompts for interesting systems you may wish to try out

## "DuckDB style CLI" 

We are making make a duckdb style CLI Apache 2 licensed code. 
nice display formatting that can read parquet files and run the ClickBench
benchmarks.

Example usage:

(TODO find a publicly accessible dataset to read from)

```sql
yadb> select * from parquet_scan('data.parquet') limit 5;
```

Example output;

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

Features:
1. Read Parquet files from local or remote systems
2. Support SQL queries (SELECT, WHERE, LIMIT)
3. Display results in a tabular format


Additional potential options:
1. Add metrics to the CLI to show query execution time and resource usage such as memory, CPU and network bandwidth usage

## "TPC benchmark data in Vortex format"

We are making a command line tool that generates TPC-H and TPC-DS
benchmark data as Vortex format files, for a user specified scale
factor and output directory.

Example usage:

```shell
tpc2vortex --benchmark tpch --scale-factor 1 --output-dir ./data
```

Example output:

```
Created ./data/nation.vortex (25 rows)
Created ./data/region.vortex (5 rows)
Created ./data/customer.vortex (150000 rows)
...
Created ./data/lineitem.vortex (6001215 rows)
Generated TPC-H scale factor 1 in 12.3s
```

Features:
1. Generate TPC-H data at any scale factor
2. Generate TPC-DS data at any scale factor
3. Write each table as a Vortex file
4. Report per-table row counts and total generation time

Additional potential options:
1. Verify the output by querying the generated files with DataFusion and
   checking row counts against the expected values for the scale factor
2. Parallel generation of tables

Recipes: [base](recipes/base.md), [tpch](recipes/tpch.md),
[vortex](recipes/vortex.md)
