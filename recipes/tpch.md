# TPC-H data generation

Quickly generate standard TPC-H benchmark datasets using
[tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs).

## Dependencies

For shell use (the [Library use](#library-use-embedding-a-generator-in-your-own-tool)
section below has the `cargo add` commands for embedding):

```shell
cargo install tpchgen-cli@2.0.2
```

## Versions

Required arrow / DataFusion versions per tpchgen release (the `tpchgen`
and `tpchgen-arrow` crates are versioned in lockstep; verified
2026-08-31; full cross-crate matrix in [base](base.md)):

| tpchgen / tpchgen-arrow | needs arrow | compatible datafusion |
|------------------------:|------------:|----------------------:|
| 3.0.0 (latest) | 59 | 55.0.0 |
| 2.0.2 (this recipe) | 57 | 51.0.0 – 52.0.0 |
| 1.1.1 | 54 | 46.0.0 |

This recipe pins 2.0.2 (arrow 57) because that is the only arrow major
also supported by the [vortex](vortex.md) recipe.

## Code

Generate scale factor 1 data as Parquet:

```shell
tpchgen-cli --scale-factor 1 --format parquet
```

## Verify

TODO: generate data and check the expected files exist (e.g.
`lineitem.parquet`) and row counts match the scale factor.

## Library use (embedding a generator in your own tool)

`tpchgen-cli` is for shell use. To generate arrow `RecordBatch`es inside
a Rust program, use the library crates instead:

```shell
cargo add tpchgen@2.0.2 tpchgen-arrow@2.0.2   # arrow ^57.1
```

```rust
use tpchgen::generators::LineItemGenerator;
use tpchgen_arrow::{LineItemArrow, RecordBatchIterator};

// (scale_factor, part, part_count) — part is 1-based; each table has
// XxxGenerator + XxxArrow pairs (Nation, Region, Part, Supplier,
// PartSupp, Customer, Order, LineItem)
let batches = LineItemArrow::new(LineItemGenerator::new(1.0, 1, 1));
let schema = batches.schema().clone();
for batch in batches { /* RecordBatch */ }
```

Pick the tpchgen version to match the project's arrow major version —
see [Versions](#versions) above.

## Notes

- TPC-DS is NOT supported by tpcgen-rs — see the
  [tpcds](../future-recipes/tpcds.md) future recipe for status.
- The arrow / DataFusion versions in the table above only matter for
  library use; `tpchgen-cli` writes Parquet/TBL/CSV files, which any
  DataFusion version can read.
