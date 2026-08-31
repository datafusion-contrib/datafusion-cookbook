# AGENTS.md

Instructions for coding agents building custom analytic systems from
[Apache DataFusion](https://datafusion.apache.org/) using this cookbook.

## How to use this repo

1. Start from the user's prompt (examples in [prompts/](prompts/)).
2. Scaffold the base project using [recipes/base.md](recipes/base.md).
3. Pick the recipes from `recipes/` that match the requested features.
   (`future-recipes/` holds incomplete recipes — use them as starting
   points only, and expect to fill in gaps.)
4. After applying each recipe, run its **Verify** step before moving on.
   If verification fails, fix the issue before adding more recipes.
5. Build in `workdir/<project_name>/` (gitignored scratch space).

## Recipe index

See the [README.md](README.md) for tables of prompts and verified
recipes. 

Every recipe and prompt also carries YAML frontmatter (`name`, `category`,
`status`, `arrow_major`, `crates`, `datafusion`, ...) — grep it to find recipes
by category or arrow version without opening each file.

## Adding a recipe

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Rules

- Pin dependency versions exactly as the recipe states; do not upgrade
  or substitute crates without being asked.
- Prefer recipe code over your own memory of DataFusion APIs — recipes
  are verified against the pinned versions.
- If a recipe is wrong or incomplete, note it in your final report so it
  can be fixed upstream.
