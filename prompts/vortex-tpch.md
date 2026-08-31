---
name: vortex-tpch
builds: TPC-H data generator writing Vortex files
recipes: [base, cli, tpch, vortex]
---

# TPC benchmark data in Vortex format

We are making a command line tool to generate TPC-H benchmark data as Vortex
format files, for a user specified scale factor and output directory.

This tool is similar to the [`tpcgen-cli` tool] from [tpchgen-rs].


[tpchgen-rs]: https://github.com/datafusion-contrib/tpcgen-rs
[`tpcgen-cli` tool]: https://github.com/datafusion-contrib/tpcgen-rs/tree/main/tpchgen-cli

## Example usage

```shell
vortex-gen tpch --scale-factor 1 --output-dir ./data
```

## Example output

```
Created ./data/nation.vortex (25 rows)
Created ./data/region.vortex (5 rows)
Created ./data/customer.vortex (150000 rows)
...
Created ./data/lineitem.vortex (6001215 rows)
Generated TPC-H scale factor 1 in 12.3s
```

## Features

1. Generate TPC-H data at any scale factor
3. Write each table as a Vortex file
4. Report per-table row counts and total generation time

## Additional potential options

1. Multi-part generation of tables
2. Parallel generation of tables in multiple threads

## Future work:
2. Generate TPC-DS data (when TPCDS generation is published)

```shell
vortex-gen tpcds --scale-factor 10 --table catalog_sales
```