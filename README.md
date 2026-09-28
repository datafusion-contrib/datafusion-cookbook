# datafusion-cookbook

This is an dual Agent and Human cookbook for quickly assebling custom analytic
systems with [Apache DataFusion] and the [Apache Arrow] ecosystem.

[Apache DataFusion]: https://datafusion.apache.org/
[Apache Arrow]: https://arrow.apache.org/

You choose from a menu of options to assemble the system that meets your needs.

Agents and their operators should start with [AGENTS.md](AGENTS.md).

## Example prompts

Prompts describe interesting systems you may wish to build ot base your own
system on. They also illustrate the types of systems that can be built easily:

<!-- Generated from prompt frontmatter by scripts/regen_index.py; do not edit by hand -->
<!-- BEGIN GENERATED: prompt-index -->
| Prompt | Builds | Uses Recipes |
|--------|--------|--------------|
| [flightsql-server](prompts/flightsql-server.md) | Flight SQL server exposing a directory of Parquet files, queried from the command line | base, cli, flight-sql-server, flight-sql-client |
| [vortex-tpch](prompts/vortex-tpch.md) | TPC-H data generator writing Vortex files | base, cli, tpch, vortex |
<!-- END GENERATED: prompt-index -->

## Recipes (Menu)

Every project starts with the [base](recipes/base.md) recipe, then adds
ingredients from the menu below.

<!-- Generated from recipe frontmatter by scripts/regen_index.py; do not edit by hand -->
<!-- BEGIN GENERATED: recipe-index -->
| Recipe | Category | Provides | Verified |
|--------|----------|----------|----------|
| [base](recipes/base.md) | Base | New cargo project with DataFusion | ✅ |
| [cli](recipes/cli.md) | REPL / CLI | Command line argument parsing with clap | ✅ |
| [vortex](recipes/vortex.md) | File Formats | Read and write Vortex files | ✅ |
| [tpch](recipes/tpch.md) | Data Generation | Generate TPC-H benchmark datasets | ✅ |
| [flight-sql-client](recipes/flight-sql-client.md) | Wire Transport | Query a Flight SQL server from the command line | ✅ |
| [flight-sql-server](recipes/flight-sql-server.md) | Wire Transport | Serve DataFusion over Arrow Flight SQL | ✅ |
<!-- END GENERATED: recipe-index -->

## Future recipes

Recipes that are still in progress can be found in the
[future-recipes/](future-recipes/) directory. We would welcome your help via
issues or pull requests in completing them. See [TESTING.md](TESTING.md) for
instructions on how to run the Verify step for a recipe.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add a new recipe or
prompt.
