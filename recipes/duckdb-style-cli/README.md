# yadb — a DuckDB-style SQL CLI

Built for the ["DuckDB style CLI" prompt](../../PROMPTS.md):

> Make a DuckDB style CLI using Apache 2 licensed code, with nice display
> formatting, that can read Parquet and JSON files.

Roughly 250 lines of Rust. Everything it does comes from the engine; the code
is argument handling, a REPL loop, and error plumbing.

## Run it

```console
$ cargo run -- data/trips.parquet
Registered data/trips.parquet as table "trips"
yadb 0.1.0 — DataFusion 54.1.0. Type .help for help, Ctrl-D to exit.
yadb> select tpep_pickup_datetime, passenger_count, total_amount
  ...> from trips
  ...> limit 5;
+----------------------+-----------------+--------------+
| tpep_pickup_datetime | passenger_count | total_amount |
+----------------------+-----------------+--------------+
| 2024-01-24T15:17:12  | 1               | 27.5         |
| 2024-01-24T15:52:24  | 1               | 18.37        |
| 2024-01-24T15:08:55  | 1               | 35.28        |
| 2024-01-24T15:42:55  | 1               | 15.96        |
| 2024-01-24T15:52:23  | 1               | 26.88        |
+----------------------+-----------------+--------------+
(5 rows, 0.042s)
```

Files can also be queried by path with nothing registered:

```console
$ cargo run
yadb> select count(*) from 'data/trips.parquet';
+----------+
| count(*) |
+----------+
| 2964624  |
+----------+
(1 row, 0.013s)
```

Test data — 48 MB, no credentials:

```shell
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

Dot commands: `.tables`, `.schema <table>`, `.help`, `.quit`.

## Ingredients used

| Ingredient | Role |
|------------|------|
| [DataFusion Core](../../ingredients/datafusion-core.md) | Engine, SQL planning, execution |
| [REPL Line Editing](../../ingredients/rustyline.md) | History, keybindings, multi-line input |
| [Table-Formatted Output](../../ingredients/pretty-printing.md) | The `+----+` output |
| [Parquet](../../ingredients/parquet.md) | Parquet reader |
| [JSON](../../ingredients/json.md) | NDJSON reader |

Three dependencies: `datafusion`, `tokio`, `rustyline`.

## Acceptance criteria

Against the numbered criteria in `PROMPTS.md`:

| # | Criterion | Status |
|---|-----------|--------|
| 1 | Reads Parquet from a local path | met |
| 2 | Reads newline-delimited JSON | met |
| 3 | SELECT / WHERE / LIMIT / GROUP BY / aggregates | met |
| 4 | Aligned ASCII table output | met |
| 5 | REPL: history, multi-line `;`, Ctrl-D, Ctrl-C | met; Ctrl-C and Ctrl-D verified by inspection of the `ReadlineError` arms and an EOF exit test, not by an automated interactive test |
| 6 | Permissive licences, non-Apache-2.0 called out | met — `rustyline` is MIT, called out in `Cargo.toml` and below |
| 7 | Bad SQL returns to the prompt, does not exit | met |

Optional extension 1 (query timing and row counts) is implemented. Extensions
2–4 — `EXPLAIN` passthrough, remote object stores, ClickBench timings — are
not.

### Licensing

`yadb` is Apache-2.0. `datafusion` and `tokio` are Apache-2.0. **`rustyline` is
MIT**, not Apache-2.0 — permissive and compatible, but flagged because the
prompt asks specifically for Apache-2 licensed code. Dropping it means writing
the line editor by hand and losing history and keybindings.

## Not done

- No object-store support, so no S3 or HTTP paths.
- No `EXPLAIN` passthrough beyond what DataFusion answers directly.
- No tab completion of table or column names.
- No pager, so a `SELECT *` without a `LIMIT` prints the whole result.
- No automated tests. The acceptance criteria above were verified by hand
  against the NYC taxi file and a small NDJSON fixture.
