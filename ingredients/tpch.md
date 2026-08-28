---
name: tpch
title: TPC-H Data Generation
category: data-generation
summary: Generate TPC-H benchmark datasets as Parquet, CSV, or .tbl at any scale factor, in pure Rust.
when_to_use: You need realistic multi-table test data, or the prompt asks for benchmark numbers and you have no dataset to hand.
crate: tpchgen-cli
version: "3.0.0"
datafusion: any
license: Apache-2.0
repo: https://github.com/clflushopt/tpchgen-rs
install: cargo install tpchgen-cli@3.0.0
status: stable
pitfalls:
  - "Output format is a subcommand, not a flag: `tpchgen-cli parquet -s 1`. There is no `--format` option."
  - "Scale factor is roughly gigabytes of raw data. SF=1 is fine on a laptop; SF=100 will fill a disk. Start at SF=1."
  - "A binary, not a library dependency. Generate the data as a build step, then point DataFusion at the output directory."
  - "Also distributed as a Python package, so `uvx tpchgen-cli` runs it with no Rust toolchain. Also mirrored at datafusion-contrib/tpcgen-rs, but the published crates come from the clflushopt repo."
example: https://github.com/clflushopt/tpchgen-rs#try-it-now
---

```console
$ cargo install tpchgen-cli
$ tpchgen-cli parquet -s 1 --output-dir data/tpch
```

Or without a Rust toolchain at all:

```console
$ uvx tpchgen-cli parquet -s 1 --output-dir data/tpch
```

That writes the eight TPC-H tables — `lineitem`, `orders`, `customer`,
`part`, `partsupp`, `supplier`, `nation`, `region` — which register
straightforwardly:

```rust
for table in ["lineitem", "orders", "customer", "part",
              "partsupp", "supplier", "nation", "region"] {
    ctx.register_parquet(
        table,
        format!("data/tpch/{table}.parquet"),
        ParquetReadOptions::default(),
    ).await?;
}
```

For ClickBench rather than TPC-H, the `hits.parquet` dataset is a single file
download from the [ClickBench repo](https://github.com/ClickHouse/ClickBench)
and needs no generator.
