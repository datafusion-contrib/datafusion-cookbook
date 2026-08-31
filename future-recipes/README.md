# Future recipes

This directory contains recipes that are still in progress, either due to
missing dependencies or because they are not yet fully implemented.

When a recipe here is completed and its Verify step passes, move it to
[recipes/](../recipes/) and add it to the index in the top-level
[README.md](../README.md).

<!-- Generated from recipe frontmatter by scripts/regen_index.py; do not edit by hand -->
<!-- BEGIN GENERATED: future-recipe-index -->
| Recipe | Category | Provides | Status |
|--------|----------|----------|--------|
| [repl](repl.md) | REPL / CLI | Interactive SQL REPL | not yet verified |
| [json](json.md) | File Formats | Read newline-delimited JSON files (built in to DataFusion) | not yet verified |
| [parquet](parquet.md) | File Formats | Read Parquet files (built in to DataFusion) | not yet verified |
| [zarr](zarr.md) | File Formats | Read Zarr data | integration crate TBD |
| [variant](variant.md) | Semi-structured Data | Store and query JSON-like data as Variant | not yet written |
| [tpcds](tpcds.md) | Data Generation | Generate TPC-DS benchmark datasets | blocked: `tpcdsgen-arrow` is alpha and arrow 59 only (no vortex support) |
| [arrow-version-conversion](arrow-version-conversion.md) | Interop | Convert RecordBatches between arrow versions via the C Data interface | not yet written |
| [postgres](postgres.md) | Connectors | Query PostgreSQL tables via datafusion-table-providers | not yet written |
| [adbc](adbc.md) | Wire Transport | Arrow-native database connectivity (ADBC) | not yet written |
| [arrow-flight](arrow-flight.md) | Wire Transport | Stream Arrow data over gRPC with Arrow Flight | not yet written |
| [flight-sql](flight-sql.md) | Wire Transport | Serve SQL to JDBC/ODBC/ADBC clients via Flight SQL | not yet written |
| [pgwire](pgwire.md) | Wire Transport | Serve any Postgres client via the Postgres wire protocol | not yet written |
| [observability](observability.md) | Observability | Trace queries with datafusion-tracing / OpenTelemetry | not yet written |
<!-- END GENERATED: future-recipe-index -->
