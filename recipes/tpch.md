---
name: tpch
category: data-generation
provides: Generate TPC-H benchmark datasets
status: verified
verified: 2026-08-31
arrow_major: 57
crates: tpchgen@2.0.2, tpchgen-arrow@2.0.2
datafusion: 51.0.0 - 52.0.0
---

# TPC-H data generation

Quickly generate standard TPC-H benchmark datasets using
[tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs).

## Dependencies


```shell
cargo add tpchgen@2.0.2 tpchgen-arrow@2.0.2   # arrow ^57.1
```

## Versions

Required arrow / DataFusion versions per tpchgen; verified
2026-08-31; full cross-crate matrix in [base](base.md)):

| tpchgen / tpchgen-arrow | needs arrow | compatible datafusion |
|------------------------:|------------:|----------------------:|
|         3.0.0 (latest)  |         59  |              55.0.0   |
|     2.0.2 (this recipe) |          57 |       51.0.0 – 52.0.0 |
|                   1.1.1 |          54 |                46.0.0 |


## Code

```rust
use tpchgen::generators::LineItemGenerator;
use tpchgen_arrow::{LineItemArrow, RecordBatchIterator};

// All 8 generators share this exact signature: new(scale_factor: f64,
// part: i32, part_count: i32), part is 1-based (Nation and Region ignore
// the arguments — they are fixed-size). Each table is an XxxGenerator +
// XxxArrow pair: Nation, Region, Part, Supplier, PartSupp, Customer,
// Order, LineItem.
let batches = LineItemArrow::new(LineItemGenerator::new(1.0, 1, 1));
// .schema() comes from the RecordBatchIterator trait (must be in scope)
let schema = batches.schema().clone();
for batch in batches { /* RecordBatch */ }
```

## Verify

```shell
cargo install tpchgen-cli@2.0.2
```

Generate scale factor 1 data as Parquet:

```shell
tpchgen-cli --scale-factor 1 --format parquet
```

Generate data and check the expected files exist (e.g.
`lineitem.parquet`) and row counts match the scale factor.

## Notes

- The arrow / DataFusion versions in the table above only matter for
  library use; `tpchgen-cli` writes Parquet/TBL/CSV files, which any
  DataFusion version can read.
- Output file naming (tpchgen-cli convention): singular table name except
  `orders` — `OrderGenerator`/`OrderArrow` write to an `orders` file.
- Expected row counts at scale factor 1 (scale linearly, except nation=25
  and region=5 always): nation 25, region 5, supplier 10,000,
  customer 150,000, part 200,000, partsupp 800,000, orders 1,500,000,
  lineitem 6,001,215 (lineitem is approximate per spec; 6,001,215 is the
  exact SF 1 value tpchgen produces).
- Parallelizing: one thread per table caps out quickly because lineitem
  dominates generation time. For real speedups, split
  lineitem/orders using `part`/`part_count` — `new(sf, i, n)` for `i` in
  `1..=n` — and generate parts in parallel. To still produce one output
  file per table, feed the parts to a single writer in order (chain the
  iterators, or have generator threads send batches over a channel to the
  writer), which is how `tpchgen-cli` does it.
