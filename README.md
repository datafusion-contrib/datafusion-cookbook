# datafusion-cookbook

This is an Agent first cookbook. 

It contains recipes for quickly assembling custom analytic systems from Apache DataFusion

(TODO links to the datafusion website and documentation and paper)

You can choose from a menu of options to assemble a custom analytic system that
meets your needs.

Example prompts:
- "make a duckdb style CLI that uses Apache 2 licensed code with nice display formatting that can read parquet and json"

See more prompts in the [PROMPTS.md](PROMPTS.md) file.

## Recipe index (Menu)

| Recipe | Provides |
|--------|----------|
| [base](recipes/base.md) | New cargo project with DataFusion |
<!-- Add one row per verified recipe. Do not list unverified recipes. -->

# Menu Options

## Base

Every project should start with the base recipe to build a project scaffold with DataFusion.

| Name | Description                        | Required Dependency | Install / Usage Instructions |
|------|:-----------------------------------|---------------------|------------------------------|
| base | Scaffold a new DataFusion project  | None                | [base.md](recipes/base.md)   |

## Additional Ingredients

### REPL Scaffolding

| Format | Description      | Required Dependency | Install / Usage Instructions |   |   |
|--------|:-----------------|---------------------|------------------------------|---|---|
| TODO   | Basic CLI / REPL |                     | TODO                         |   |   |

## Data Generation

| Format       | Description                                    | Required Dependency                   | Install / Usage Instructions |   |   |
|--------------|:-----------------------------------------------|---------------------------------------|------------------------------|---|---|
| TPC-H/TPC-DS | Fast generation of standard benchmark datasets | https://github.com/datafusion-contrib/tpcgen-rs | TODO                         |   |   |


| Format  | Description | Required Dependency          | Install / Usage Instructions |   |   |
|---------|:------------|------------------------------|------------------------------|---|---|
| Parquet |             | None (built with DataFusion) |                              |   |   |
| JSON    |             | None (built with DataFusion) |                              |   |   |
| Vortex  |             | TODO                         |                              |   |   |
| Zarr    |             | TODO                         |                              |   |   |



Future items to add to the menu:



# Instructions for adding a new recipe:
TODO