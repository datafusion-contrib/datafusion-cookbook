---
name: datafusion-cli
title: datafusion-cli (prebuilt)
category: repl
summary: An existing, Apache-2.0 licensed, DuckDB-style SQL CLI with a REPL, table-formatted output, and direct Parquet/CSV/JSON/Avro file queries.
when_to_use: Before building a CLI from scratch. If the requirement is "a DuckDB-style CLI over Parquet", this already is one — build only if you need behaviour it lacks.
crate: datafusion-cli
version: "54.1.0"
datafusion: "54"
license: Apache-2.0
repo: https://github.com/apache/datafusion/tree/main/datafusion-cli
install: cargo install datafusion-cli@54.1.0
status: stable
pitfalls:
  - "This is a binary, not a library. You cannot depend on it to build your own CLI; read its source as a reference instead."
  - "Installs the `datafusion-cli` binary. `cargo install` builds from source and takes several minutes."
example: https://datafusion.apache.org/user-guide/cli/overview.html
---

Worth checking first, because a surprising number of "build me a DuckDB-style
CLI" prompts are already satisfied by it:

```console
$ datafusion-cli
DataFusion CLI v54.1.0
> select * from 'data/hits.parquet' limit 5;
```

It already provides the REPL, history, the `+---+---+` table output, direct
file querying, `\d`-style commands, and `EXPLAIN`. Its
[`main.rs`](https://github.com/apache/datafusion/blob/main/datafusion-cli/src/main.rs)
is the best reference for wiring up your own: it shows how the dynamic file
catalog, the object store registry, and the `parquet_metadata` table function
are registered.

If you are building your own CLI because you need different behaviour, take
[REPL Line Editing](rustyline.md) and [Pretty Printing](pretty-printing.md)
rather than starting from an empty `main`.
