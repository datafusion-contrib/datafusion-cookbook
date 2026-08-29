# Recipes

A **recipe** is a complete, working system assembled from
[ingredients](../ingredients/). An ingredient is a building block; a recipe is
a thing you can run.

Each recipe answers one prompt from [PROMPTS.md](../PROMPTS.md) and records,
in its own README, which acceptance criteria it meets and what building it
cost. That last part is the point: a recipe that compiles is useful, but a
recipe that says *where the time went* is what improves the cookbook.

| Recipe | Prompt | Ingredients |
|--------|--------|-------------|
| [duckdb-style-cli](duckdb-style-cli/) | "DuckDB style CLI" | datafusion-core, rustyline, pretty-printing, parquet, json |

## Adding a recipe

1. Build it against a prompt in [PROMPTS.md](../PROMPTS.md). If there is no
   prompt for what you are building, add one first — with acceptance criteria,
   or there is nothing to check the result against.
2. Keep dependencies to what the prompt needs. A recipe carrying an unused
   crate teaches the wrong lesson.
3. Write a README covering: how to run it, ingredients used, a criterion-by-
   criterion status table, and **what building it taught you**.
4. Turn each lesson into a pitfall on the relevant ingredient and regenerate:
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
