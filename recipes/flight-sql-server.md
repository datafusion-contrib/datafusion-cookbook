---
name: flight-sql-server
category: wire-transport
provides: Serve DataFusion over Arrow Flight SQL
status: verified
verified: 2026-09-03
arrow_major: 58
crates: datafusion@54.1.0, datafusion-flight-sql-server@0.4.18, datafusion-federation@=0.5.5
datafusion: 54.0.0 - 54.1.0
---

# Flight SQL server

Serve a DataFusion `SessionContext` over Arrow Flight SQL, so any Flight SQL
client can query it as a database.

## Dependencies

```shell
cargo add datafusion@54.1.0 datafusion-flight-sql-server@0.4.18
cargo add datafusion-federation@=0.5.5   # exact pin — see Versions
cargo add tokio@1 --features full
```

The `=0.5.5` on `datafusion-federation` is required, not stylistic.

## Versions

This recipe is arrow 58 / DataFusion 54. `datafusion-flight-sql-server`
0.4.18 requires `datafusion ^54.0` and `arrow-flight ^58.3`, so it cannot be
used with the arrow 59 / DataFusion 55 default in [base](base.md).

| datafusion-flight-sql-server | needs datafusion | needs arrow |
|-----------------------------:|-----------------:|------------:|
|      0.4.18 (this recipe)    |           ^54.0  |          58 |
|                       0.4.17 |           ^54.0  |          58 |
|                       0.4.16 |           ^53.0  |          58 |

**`datafusion-federation` must be pinned to `=0.5.5`.** The server depends on
`datafusion-federation ^0.5.5`, and 0.5.6 raised its own requirement from
`datafusion ^54` to `datafusion ^55` in a patch release. Cargo therefore
selects 0.5.6 by default and resolves a graph containing *both* DataFusion 54
and 55 (and both arrow 58 and 59):

```console
$ cargo tree -i datafusion-common@55.0.0
datafusion-common v55.0.0
├── datafusion v55.0.0
│   └── datafusion-federation v0.5.6
│       └── datafusion-flight-sql-server v0.4.18
```

Check for this before debugging type errors:

```shell
grep -A1 '^name = "datafusion"$' Cargo.lock   # expect exactly one version
```

## Code

```rust
use datafusion::prelude::*;
use datafusion_flight_sql_server::service::FlightSqlService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // One table per Parquet file, named after the file stem.
    let mut registered = 0;
    for entry in std::fs::read_dir("./data")? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let name = path.file_stem().and_then(|s| s.to_str()).ok_or("bad name")?.to_string();
        ctx.register_parquet(&name, path.to_str().ok_or("bad path")?, ParquetReadOptions::default())
            .await?;
        println!("registered {name}");
        registered += 1;
    }

    let addr = "127.0.0.1:50051";
    println!("serving {registered} table(s) on {addr}");

    // Runs until killed.
    FlightSqlService::new(ctx.state()).serve(addr.to_string()).await?;
    Ok(())
}
```

## Verify

Put any Parquet file in `./data`, then start the server:

```console
$ cargo run --release
registered trips
serving 1 table(s) on 127.0.0.1:50051
```

From another terminal, query it with the client from
[flight-sql-client](flight-sql-client.md):

```console
$ flight_sql_client --host 127.0.0.1 --port 50051 \
    statement-query "select count(*) from trips"
+----------+
| count(*) |
+----------+
| 2964624  |
+----------+
```

Expected: the row count of the Parquet file. The count above is a 2.9M-row
NYC taxi file; yours will differ.

## Notes

- `FlightSqlService::new` takes the `SessionState` (`ctx.state()`), not the
  `SessionContext`.
- The service speaks gRPC over HTTP/2. Behind a proxy that only handles
  HTTP/1.1 the connection fails in a way that reads like an auth error.
- Clients need not share this project's arrow version — see
  [flight-sql-client](flight-sql-client.md).
- For the PostgreSQL wire protocol instead, so `psql` connects without an
  Arrow-aware driver, see the `pgwire` recipe.
