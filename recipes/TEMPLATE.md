<!-- 
This file contains the structure that new recipes should follow

Any new recipe should have
1. a prompt that uses it in the [prompts/](../prompts/) directory, to show how it can be used
2. frontmatter like the block below — the index tables in
   [README.md](../README.md) and [future-recipes/README.md](../future-recipes/README.md)
   are generated from it by `python3 scripts/regen_index.py`; run that
   script instead of editing the tables by hand

-->

---
name: <kebab-case-slug>
category: <one of: base | repl-cli | file-formats | semi-structured-data | data-generation | interop | connectors | wire-transport | observability>
provides: <one line shown in the index tables>
status: draft                  # draft | blocked | verified
status_note: not yet written   # free text shown in the future-recipes index
arrow_major: TBD               # arrow major version required, e.g. 57, or "any"
crates: TBD                    # exact pins, e.g. foo@1.2.3, bar@4.5.6 (or "none")
datafusion: TBD                # compatible datafusion versions, e.g. 51.0.0 - 52.0.0
---

# <Recipe name>

One-sentence description of what this adds.

## Dependencies

Exact `cargo add` commands, with versions.

## Versions

Table mapping this recipe's crate versions to the arrow (and compatible
DataFusion) major versions they require. All arrow-based crates in a
project must share one arrow major version — see the master
cross-recipe table in [base](base.md).

## Code

Minimal working snippet(s) and where they go.

## Verify

Command(s) to run and the expected output. The recipe is not complete
until this passes.

## Notes

Known limitations, version caveats, links to upstream docs.
