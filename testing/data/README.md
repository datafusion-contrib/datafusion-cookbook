# Test fixtures

Small, deterministic data files that recipe **Verify** steps query, so every
run checks against the same exact results.

## trips.parquet

One `passenger_count` column (`Int32`, nullable) with six rows: `0, 1, 1, 2,
2, 2`.

| query | result |
|-------|--------|
| `select count(*) from trips` | `6` |
| `select passenger_count, count(*) from trips where passenger_count > 0 group by passenger_count order by passenger_count` | `(1, 2)`, `(2, 3)` |

Regenerate with [datafusion-cli](https://datafusion.apache.org/user-guide/cli/):

```shell
datafusion-cli -c "COPY (SELECT CAST(column1 AS INT) AS passenger_count FROM (VALUES (0),(1),(1),(2),(2),(2))) TO 'testing/data/trips.parquet' STORED AS PARQUET;"
```
