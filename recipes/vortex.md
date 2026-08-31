---
name: vortex
category: file-formats
provides: Read and write Vortex files
status: verified
verified: 2026-08-31
arrow_major: 57
crates: vortex@0.65.0, arrow@57
datafusion: 51.0.0 - 52.0.0
---

# Vortex

Read and write files in the [Vortex](https://github.com/spiraldb/vortex)
columnar file format.

See the [Vortex DataFusion integration guide](https://docs.vortex.dev/developer-guide/integrations/datafusion) for details

## Dependencies

```shell
cargo add vortex@0.65.0 arrow@57
```

Vortex uses `arrow::datatypes::SchemaRef` and `arrow::array::RecordBatch`
directly, so `arrow` must be a direct dependency — pin it to the same arrow
major as vortex (57 here) so cargo resolves a single copy.

## Versions

Required arrow / DataFusion versions per vortex release (verified
2026-08-31; full cross-crate matrix in [base](base.md)):

| vortex | needs arrow | compatible datafusion |
|-------:|------------:|----------------------:|
| 0.67.0 – 0.85.0 (latest) | 58 | 53.0.0 – 54.0.0 |
| 0.65.0 – 0.66.0 (this recipe) | 57 | 51.0.0 – 52.0.0 |

## Code

Write arrow `RecordBatch`es to a `.vortex` file and read the row count
back, with no tokio dependency (vortex default features suffice):

```rust
use std::path::Path;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::iter::ArrayIteratorAdapter;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

/// Write an iterator of same-schema RecordBatches to `path`; returns rows written.
fn write_vortex_file<I>(
    schema: arrow::datatypes::SchemaRef,
    batches: I,
    path: &Path,
) -> Result<u64, Box<dyn std::error::Error>>
where
    I: Iterator<Item = arrow::array::RecordBatch> + Send + 'static,
{
    let runtime = CurrentThreadRuntime::new();
    // Without .with_handle() the session PANICS at runtime in write_options()
    let session = VortexSession::default().with_handle(runtime.handle());

    // nullable=false must match the `false` in from_arrow below
    let dtype = DType::from_arrow(schema);
    let arrays = ArrayIteratorAdapter::new(
        dtype,
        batches.map(|batch| ArrayRef::from_arrow(&batch, false)),
    );

    let file = std::fs::File::create(path)?;
    let summary = session.write_options().blocking(&runtime).write(file, arrays)?;
    Ok(summary.row_count())
}

/// Row count from the file footer (no data decode).
fn count_rows(path: &Path) -> Result<u64, Box<dyn std::error::Error>> {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime.block_on(session.open_options().open_path(path))?;
    Ok(file.row_count())
}
```

Alternative (async): enable the vortex `tokio` feature, construct
`VortexSession::default()` *inside* a tokio runtime, and use
`session.write_options().write(&mut tokio_file, stream).await`.

## Verify

Write a small RecordBatch through `write_vortex_file`, then confirm
`count_rows` returns the same number of rows. (The code above has been
verified end-to-end as written: a TPC-H SF 1 lineitem — 6,001,215 rows —
round-trips through write and footer read-back. Do not rewrite it; use
it verbatim.)

## Notes

- The vortex API changes between versions — this code is verified against
  0.65.0 exactly; do not trust docs or memory for other versions.
- Full scan (not just footer count): `file.scan()?.into_array_iter(&runtime)`;
  DataFusion table provider lives in the separate `vortex-datafusion` crate
  (0.65.0 requires `datafusion ^52`, which is also on arrow 57, so the
  versions line up — not yet verified here).
- TPC-DS blocker: `tpcdsgen-arrow 0.1.0-alpha.1` needs arrow 59; no vortex
  release supports arrow 59, so TPC-DS-to-vortex is currently impossible.
