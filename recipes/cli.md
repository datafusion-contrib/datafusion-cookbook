---
name: cli
category: repl-cli
provides: Command line argument parsing with clap
status: verified
verified: 2026-08-31
arrow_major: any
crates: clap@4.6.6
datafusion: any
---

# Command line argument parsing

Parse command line arguments (subcommands, flags, defaults) with
[clap](https://docs.rs/clap)'s derive API.

## Dependencies

```shell
cargo add clap --features clap/derive   # resolves to clap 4.x
```

## Versions

clap is not arrow-based, so it works with every arrow / DataFusion
combination in the [base](base.md) matrix (verified with clap 4.6.6 on
2026-08-31).

## Code

A binary with one subcommand and typed flags with defaults:

```rust
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "my-tool", about = "One-line description")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Doc comments become the subcommand's --help text
    Generate {
        /// Scale factor (e.g. 1, 10, 100)
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,
        /// Directory to write into (created if missing)
        #[arg(long, default_value = "./data")]
        output_dir: PathBuf,
        /// Boolean flag, off by default
        #[arg(long)]
        sequential: bool,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate { scale_factor, output_dir, sequential } => {
            // ...
        }
    }
}
```

## Verify

```shell
cargo run -- generate --help
cargo run -- generate --scale-factor 10
```

Expected: the first prints the flags with their doc comments and
defaults; the second parses without error. Unknown flags and malformed
values exit non-zero with a usage message automatically.

## Notes

- Multi-word field names map to kebab-case flags (`scale_factor` →
  `--scale-factor`).
- The derive feature is required for `#[derive(Parser)]`; without it
  only the builder API is available.
