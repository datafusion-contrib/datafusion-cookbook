---
name: repl
category: repl-cli
provides: Interactive SQL REPL
status: draft
status_note: not yet verified
arrow_major: TBD
crates: TBD
---

# REPL

An interactive SQL REPL (read-eval-print loop): a prompt that reads a
query, runs it with DataFusion, prints the results, and loops.

## Dependencies

TODO: not yet determined how best to build a REPL (candidates include a
line editor crate such as `rustyline`, or reusing parts of
`datafusion-cli`).

## Versions

TBD — a line editor crate adds no arrow-based dependencies, but reusing
`datafusion-cli` internals would pin a DataFusion version; see the
version table in [base](../recipes/base.md).

## Code

TODO

## Verify

```shell
cargo run
```

Then at the prompt:

```sql
select 1;
```

Expected: a formatted one-row table, and the prompt returns for the
next query.

## Notes

- Used by the [duckdb](../prompts/duckdb.md) and
  [vortex-cli](../prompts/vortex-cli.md) prompts.
- TODO: multi-line statements, command history, ctrl-c handling, query
  timing display.
