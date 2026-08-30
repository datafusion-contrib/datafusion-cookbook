# TPC-H / TPC-DS data generation

Quickly generate standard benchmark datasets using
[tpchgen-rs](https://github.com/datafusion-contrib/tpcgen-rs).

## Dependencies

```shell
cargo install tpchgen-cli
```

## Code

Generate scale factor 1 data as Parquet:

```shell
tpchgen-cli --scale-factor 1 --format parquet
```

## Verify

TODO: generate data and check the expected files exist (e.g.
`lineitem.parquet`) and row counts match the scale factor.

## Notes

- Used by the [vortex-tpch](../prompts/vortex-tpch.md) prompt.
- TODO: pin a tpchgen-cli version; confirm TPC-DS support status.
