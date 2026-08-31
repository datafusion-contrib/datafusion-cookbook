<!-- 
This file contains the structure that new recipes should follow

Any new recipe should have
1. a prompt that uses it in the [prompts/](../prompts/) directory, to show how it can be used
2. a row in the recipe index in [README.md](../README.md)

-->

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
