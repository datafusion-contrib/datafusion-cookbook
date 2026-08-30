# datafusion-cookbook

This is an Agent first cookbook. 

It contains recipes for quickly assembling custom analytic systems from Apache DataFusion

(TODO links to the datafusion website and documentation and paper)

You can choose from a menu of options to assemble a custom analytic system that
meets your needs.

Agents (and their operators) should start with [AGENTS.md](AGENTS.md).

## Example prompts

Prompts describe interesting systems you may wish to build, and which
recipes they use:

| Prompt | Builds | Uses Recipes |
|--------|--------|--------------|
| [duckdb](prompts/duckdb.md) | DuckDB style interactive SQL CLI | base, repl, parquet |
| [vortex-tpch](prompts/vortex-tpch.md) | TPC-H / TPC-DS data generator writing Vortex files | base, tpch, vortex |
| [vortex-cli](prompts/vortex-cli.md) | datafusion-cli style SQL CLI that reads/writes Vortex files | base, repl, parquet, vortex |

## Recipe index (Menu)

Every project starts with the [base](recipes/base.md) recipe, then adds
ingredients from the menu below.

| Recipe | Category | Provides | Verified |
|--------|----------|----------|:--------:|
| [base](recipes/base.md) | Base | New cargo project with DataFusion | ✅ |
| [repl](recipes/repl.md) | REPL / CLI | Interactive SQL REPL | TODO |
| [parquet](recipes/parquet.md) | File Formats | Read Parquet files (built in to DataFusion) | TODO |
| [json](recipes/json.md) | File Formats | Read newline-delimited JSON files (built in to DataFusion) | TODO |
| [vortex](recipes/vortex.md) | File Formats | Read Vortex files | TODO |
| [zarr](recipes/zarr.md) | File Formats | Read Zarr data | TODO |
| [tpch](recipes/tpch.md) | Data Generation | Generate TPC-H / TPC-DS benchmark datasets | TODO |

Future menu items: semi-structured data (Variant), observability
(datafusion-tracing / OpenTelemetry), connectors (PostgreSQL via
datafusion-table-providers), wire transport (Arrow Flight, Flight SQL,
ADBC, Postgres wire protocol).

## Adding a new recipe or prompt

Copy [recipes/TEMPLATE.md](recipes/TEMPLATE.md) or
[prompts/TEMPLATE.md](prompts/TEMPLATE.md) and follow the instructions
in it: fill in every section and add a row to the matching index above.
Mark a recipe Verified only once its Verify step passes as written.
