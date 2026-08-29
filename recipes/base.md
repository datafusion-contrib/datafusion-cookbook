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

## Notes

- TODO: pin and CI-test against the latest DataFusion release.
