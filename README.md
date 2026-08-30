# datafusion-cookbook

This is an Agent first cookbook.

It contains recipes for quickly assembling custom analytic systems from
[Apache DataFusion](https://datafusion.apache.org/) — an extensible query engine
built on Apache Arrow. See the
[library user guide](https://datafusion.apache.org/library-user-guide/) and the
[SIGMOD paper](https://dl.acm.org/doi/10.1145/3626246.3653368) for background.

You can choose from a menu of options to assemble a custom analytic system that
meets your needs.

Example prompts:

- "make a duckdb style CLI that uses Apache 2 licensed code with nice display formatting that can read parquet and json"

See more prompts in the [PROMPTS.md](PROMPTS.md) file, and how we judge the
results in [TESTING.md](TESTING.md).

## How to use this repo

**If you are an agent:** read [llms.txt](llms.txt). It is the whole menu —
every ingredient, its install command, and its known pitfalls — in one file.

**If you are a human:** the menu below links to one file per ingredient, and
[`recipes/`](recipes/) holds complete worked systems built from them —
start with [duckdb-style-cli](recipes/duckdb-style-cli/) if you want to see
the ingredients assembled into something that runs.

Each ingredient in [`ingredients/`](ingredients/) records what it is, when to
reach for it, the exact crate version this cookbook is tested against, and the
mistakes people make with it. The menu below and `llms.txt` are both
generated from those files by [`dev/generate.py`](dev/generate.py) — edit the
ingredient, not the generated output.

## Version Compatibility

**This cookbook pins DataFusion 54.1.0, not the newest release.**

DataFusion 55.0.0 is out, but nearly every crate in the surrounding ecosystem
still requires `datafusion ^54`:

| Crate | Requires |
|-------|----------|
| `datafusion-table-providers` 0.13.1 | datafusion ^54.0 |
| `datafusion-functions-json` 0.54.2 | datafusion ^54 |
| `datafusion-postgres` 0.18.0 | datafusion ^54 |
| `datafusion-federation` 0.5.5 | datafusion ^54 |
| `datafusion-flight-sql-server` 0.4.18 | datafusion ^54.0 |
| `vortex-datafusion` 0.85.0 | datafusion-* ^54 |

DataFusion majors are not interchangeable. Mixing 54 and 55 puts two copies of
the crate in one dependency graph, and the failure is a wall of
`expected SessionContext, found SessionContext` errors that reads like a bug in
your code. Take DataFusion 55 only if core plus `datafusion-tracing` is all you
need; anything else on this menu requires 54.

`python3 dev/check_versions.py` verifies every pin against crates.io, so this
table can be re-derived rather than trusted.

# Menu Options

## Base

1. Install rust (TODO in a container or on your system)

2. Create a new cargo project:

```shell
mkdir -p workdir
cd workdir
cargo new <project_name> --bin
cd <project_name>
```

3. Add the base DataFusion dependency to your Cargo.toml file:

```shell
cargo add datafusion@54.1.0 tokio@1 --features tokio/full
```

4. Confirm it works before adding anything else:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("SELECT 1").await?.show().await?;
    Ok(())
}
```

## Additional Ingredients

<!-- BEGIN GENERATED MENU -->

### Base

Start here. Every system needs this.

| Ingredient | Description | Dependency | DF | Status |
|------------|-------------|------------|----|--------|
| [DataFusion Core](ingredients/datafusion-core.md) | The query engine itself — SQL and DataFrame APIs, Arrow memory model, built-in Parquet/CSV/JSON/Avro readers. | `datafusion@54.1.0` | 54 | stable |

### REPL Scaffolding

Turning the engine into something a human can type at.

| Ingredient | Description | Dependency | DF | Status |
|------------|-------------|------------|----|--------|
| [REPL Line Editing](ingredients/rustyline.md) | Readline-style input for a CLI — history, emacs/vi keybindings, multi-line editing, completion hooks. | `rustyline@18.0.1` | any | stable |
| [Table-Formatted Output](ingredients/pretty-printing.md) | Render Arrow RecordBatches as an aligned ASCII table, the `+----+-------+` format familiar from DuckDB and psql. | None (built in) | 54 | stable |
| [datafusion-cli (prebuilt)](ingredients/datafusion-cli.md) | An existing, Apache-2.0 licensed, DuckDB-style SQL CLI with a REPL, table-formatted output, and direct Parquet/CSV/JSON/Avro file queries. | `datafusion-cli@54.1.0` | 54 | stable |

### File Formats

What you can read and write.

| Ingredient | Description | Dependency | DF | Status |
|------------|-------------|------------|----|--------|
| [JSON (newline-delimited)](ingredients/json.md) | Read newline-delimited JSON (NDJSON) files with inferred schemas. | None (built in) | 54 | stable |
| [JSON Functions](ingredients/json-functions.md) | SQL functions for querying JSON held in string columns — `json_get`, `json_get_str`, `json_contains`, and the `->` operator. | `datafusion-functions-json@0.54.2` | 54 | stable |
| [Parquet](ingredients/parquet.md) | Read and write Parquet, with predicate and projection pushdown, row-group pruning, and page-index filtering. | None (built in) | 54 | stable |
| [Variant](ingredients/variant.md) | UDFs for the open Variant binary type — semi-structured data with a compact binary encoding rather than reparsed text. | `datafusion-variant@0.1.0` | 52 | ⚠️ experimental |
| [Vortex](ingredients/vortex.md) | A next-generation compressed columnar format with random access and pushdown, positioned as a faster alternative to Parquet. | `vortex-datafusion@0.85.0` | 54 | ⚠️ experimental |
| [Zarr](ingredients/zarr.md) | Chunked, compressed N-dimensional array storage, the standard format in scientific and geospatial computing. | git: https://github.com/datafusion-contrib/arrow-zarr | 54 | ⚠️ unpublished |

### Data Generation

Test data, when you have none.

| Ingredient | Description | Dependency | DF | Status |
|------------|-------------|------------|----|--------|
| [TPC-H Data Generation](ingredients/tpch.md) | Generate TPC-H benchmark datasets as Parquet, CSV, or .tbl at any scale factor, in pure Rust. | `tpchgen-cli@3.0.0` | any | stable |

### Observability

Seeing what the engine is doing.

| Ingredient | Description | Dependency | DF | Status |
|------------|-------------|------------|----|--------|
| [OpenTelemetry Tracing](ingredients/tracing.md) | Wrap query execution in tracing spans, record native DataFusion metrics, and export to Jaeger, DataDog, or any OTLP collector. | `datafusion-tracing@54.0.0` | 54 | stable |

### Connectors

Querying systems that are not files.

| Ingredient | Description | Dependency | DF | Status |
|------------|-------------|------------|----|--------|
| [PostgreSQL](ingredients/postgres-connector.md) | Query live PostgreSQL tables from DataFusion SQL, with joins and filters pushed down to the database. | `datafusion-table-providers@0.13.1` + `postgres` | 54 | stable |
| [Query Federation](ingredients/federation.md) | Push joins, filters, and TopK queries down into the remote engine that owns the data instead of pulling rows into DataFusion. | `datafusion-federation@0.5.5` | 54 | stable |

### Wire Transport

Letting other processes query you.

| Ingredient | Description | Dependency | DF | Status |
|------------|-------------|------------|----|--------|
| [ADBC Client](ingredients/adbc.md) | Arrow Database Connectivity — a JDBC/ODBC-shaped client API that returns Arrow batches instead of rows. | `adbc_core@0.24.0` | any | stable |
| [Arrow Flight](ingredients/arrow-flight.md) | The raw gRPC protocol for moving Arrow batches between processes, underneath Flight SQL. | `arrow-flight@58.4.0` + `flight-sql` | 54 | stable |
| [Flight SQL Server](ingredients/flight-sql-server.md) | Serve DataFusion over Arrow Flight SQL, so JDBC/ODBC/ADBC clients can connect to it as a database. | `datafusion-flight-sql-server@0.4.18` | 54 | stable |
| [PostgreSQL Wire Protocol](ingredients/postgres-wire.md) | Expose DataFusion over the PostgreSQL wire protocol so psql, DBeaver, Metabase, and Grafana connect without any Arrow-aware driver. | `datafusion-postgres@0.18.0` | 54 | stable |

<!-- END GENERATED MENU -->

Future items to add to the menu:

- Custom `TableProvider` implementations
- The UDF / UDAF / UDWF / UDTF family
- Filter and projection pushdown
- Catalog and object-store integration (S3, GCS, Azure)
- Custom optimizer and analyzer rules
- Distributed execution (`datafusion-distributed`)
- Materialized views (`datafusion-materialized-views`)

# Contributing

Add a file to `ingredients/` (copy an existing one; the fields are listed in
[ingredients/README.md](ingredients/README.md)), then:

```shell
python3 dev/check_versions.py <name>   # verify the version — do not guess
python3 dev/generate.py                # regenerate the menu and llms.txt
```

Commit the regenerated `README.md` and `llms.txt` alongside. Prompts go in
[PROMPTS.md](PROMPTS.md) and need acceptance criteria, or there is nothing to
score a run against.

Struggled with something while using DataFusion? Want to share this and help
others succeed faster? Feel very welcome to contribute.
