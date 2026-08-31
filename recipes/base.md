# Base

Create a new cargo project with the core DataFusion dependency.

## Dependencies

None (requires a working Rust toolchain; see https://rustup.rs).

## Code

```shell
mkdir -p workdir
cd workdir
cargo new <project_name> --bin
cd <project_name>
cargo add datafusion@55.0.0
```

## Verify

```shell
cargo build
```

Expected: the build completes without errors.

## Versions

Every arrow-based crate in a project must share **one arrow major
version**. Since `RecordBatch`s from `arrow` 57 and one from `arrow` 59 are
different Rust types, they cannot be passed between crates. 

DataFusion pins `arrow` and it appears in the Public API, so choosing a
DataFusion version chooses the project's arrow version, and adding a crate that
needs a different arrow means downgrading DataFusion to match (or dropping it if
the project doesn't actually query anything, e.g. a pure data generator).

**Pick the DataFusion pin from your menu**: the `cargo add
datafusion@55.0.0` above (arrow 59) is right for a project with no other
arrow-based crates. A project using the [vortex](vortex.md) and/or
[tpch](tpch.md) recipes (both arrow 57) should use
`cargo add datafusion@51.0.0` instead — or skip DataFusion entirely if
nothing is queried (e.g. a pure data generator).

Known-good versions per arrow major (verified against crates.io 2026-08-31;
re-check before using new versions of DataFusion):

| arrow major |      datafusion |          vortex | tpchgen / tpchgen-arrow |
|------------:|----------------:|----------------:|------------------------:|
|          59 |        55.0.0   |           none  |                  3.0.0  |
|          58 | 53.0.0 – 54.0.0 | 0.67.0 – 0.85.0 |                    none |
|          57 | 51.0.0 – 52.0.0 | 0.65.0 – 0.66.0 |                   2.0.2 |
|          56 |          50.0.0 |            none |                    none |
|          55 | 47.0.0 – 49.0.0 |            none |                    none |
|          54 |          46.0.0 |            none |                   1.1.1 |

To check which arrow version a project resolved:

```shell
grep -A2 'name = "arrow"' Cargo.lock
```

To check a candidate crate's arrow requirement before adding it (the
crates.io API requires a User-Agent header):

```shell
curl -s -A "cookbook" \
  "https://crates.io/api/v1/crates/<crate>/<version>/dependencies" \
  | grep -o '"crate_id":"arrow[^,]*,"req":"[^"]*"'
```

## Notes
