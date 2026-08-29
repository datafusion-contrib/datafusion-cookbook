# AGENTS.md

Instructions for coding agents building custom analytic systems from
[Apache DataFusion](https://datafusion.apache.org/) using this cookbook.

## How to use this repo

1. Start from the user's prompt (examples in [PROMPTS.md](PROMPTS.md)).
2. Scaffold the base project using [recipes/base.md](recipes/base.md).
3. Pick the recipes from `recipes/` that match the requested features.
4. After applying each recipe, run its **Verify** step before moving on.
   If verification fails, fix the issue before adding more recipes.
5. Build in `workdir/<project_name>/` (gitignored scratch space).

## Recipe index

See the [README.md](README.md) for a table of verified recipes.

## Adding a recipe

Copy [recipes/TEMPLATE.md](recipes/TEMPLATE.md), fill in every section,
verify it works, then add a row to the index above.

## Rules

- Pin dependency versions exactly as the recipe states; do not upgrade
  or substitute crates without being asked.
- Prefer recipe code over your own memory of DataFusion APIs — recipes
  are verified against the pinned versions.
- If a recipe is wrong or incomplete, note it in your final report so it
  can be fixed upstream.
