# Prompts

Prompts describe complete example systems to build with the cookbook,
written as you would give them to a coding agent. 

<!-- Generated from prompt frontmatter by scripts/regen_index.py; do not edit by hand -->
<!-- BEGIN GENERATED: prompt-index -->
| Prompt                        | Builds                                                      | Uses Recipes                     |
|-------------------------------|-------------------------------------------------------------|----------------------------------|
| [duckdb](duckdb.md)           | DuckDB style interactive SQL CLI                            | base, repl, parquet              |
| [vortex-cli](vortex-cli.md)   | datafusion-cli style SQL CLI that reads/writes Vortex files | base, cli, repl, parquet, vortex |
| [vortex-tpch](vortex-tpch.md) | TPC-H data generator writing Vortex files                   | base, cli, tpch, vortex          |
<!-- END GENERATED: prompt-index -->

To add a prompt, copy [TEMPLATE.md](TEMPLATE.md), fill in the
frontmatter and every section, then regenerate the index tables with
`python3 scripts/regen_index.py`.
