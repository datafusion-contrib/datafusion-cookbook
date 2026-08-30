# Recipes

A **recipe** is a complete, working system assembled from
[ingredients](../ingredients/). An ingredient is a building block; a recipe is
a thing you can run.

Each recipe answers one prompt from [PROMPTS.md](../PROMPTS.md) and records,
in its own README, which acceptance criteria it meets and which ingredients it
used.

Anything a recipe teaches belongs on the **ingredient**, as a pitfall — not in
the recipe README. A fact written in both places drifts as soon as one is
corrected.

| Recipe | Prompt | Ingredients |
|--------|--------|-------------|
| [duckdb-style-cli](duckdb-style-cli/) | "DuckDB style CLI" | datafusion-core, rustyline, pretty-printing, parquet, json |

## Adding a recipe

1. Build it against a prompt in [PROMPTS.md](../PROMPTS.md). If there is no
   prompt for what you are building, add one first — with acceptance criteria,
   or there is nothing to check the result against.
2. Keep dependencies to what the prompt needs. A recipe carrying an unused
   crate teaches the wrong lesson.
3. Write a README covering: how to run it, ingredients used, and a
   criterion-by-criterion status table. Link the ingredients rather than
   restating what they say.
4. Turn anything you learned into a pitfall on the relevant ingredient, then
   regenerate:
   ```shell
   python3 ../dev/generate.py
   ```
   This is the compounding step. A pitfall recorded once is paid for once.
5. Confirm it builds and lints clean:
   ```shell
   cargo build && cargo clippy -- -D warnings && cargo fmt --check
   ```

Recipes are standalone crates, not workspace members, so each pins its own
dependency versions and can target a different DataFusion release as the
ecosystem moves.
