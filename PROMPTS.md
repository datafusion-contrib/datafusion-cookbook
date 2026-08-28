This file contains example prompts for interesting systems you may wish to try out

## "DuckDB style CLI" 

We are making make a duckdb style CLI Apache 2 licensed code. 
nice display formatting that can read parquet files.

Example usage:

(TODO find a publically accessable dataset to read from)

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

