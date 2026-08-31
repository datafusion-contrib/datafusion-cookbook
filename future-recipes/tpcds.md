# TPC-DS data generation

Quickly generate standard TPC-DS datasets. TPC-DS is NOT supported by
[tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs); the only
published crates are `tpcdsgen` / `tpcdsgen-arrow` (0.1.0-alpha.1,
arrow 59 only, from https://github.com/clflushopt/tpchgen-rs).

## Dependencies

TBD

## Versions

Verified 2026-08-31 (full cross-crate matrix in
[base](../recipes/base.md)):

| tpcdsgen / tpcdsgen-arrow | needs arrow | compatible datafusion |
|--------------------------:|------------:|----------------------:|
| 0.1.0-alpha.1 (only release) | 59 | 55.0.0 |

Blocker for the vortex-tpch prompt: no vortex release supports arrow 59,
so TPC-DS data cannot currently be written to Vortex files.

## Code


## Verify


## Library use (embedding a generator in your own tool)


## Notes

