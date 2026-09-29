---
name: flight-sql-server
category: wire-transport
provides: Serve DataFusion over Arrow Flight SQL
status: verified
verified: 2026-09-28
arrow_major: 59
crates: datafusion@55.1.0, datafusion-flight-sql-server@0.4.19
datafusion: 55.1.0
---

# Flight SQL server

Serve a DataFusion `SessionContext` over Arrow Flight SQL, so any Flight SQL
client can query it as a database.

## Dependencies

```shell
cargo add datafusion@55.1.0 datafusion-flight-sql-server@0.4.19
cargo add tokio@1 --features full
```

DataFusion 55.1 requires rustc 1.94 or newer.

## Versions

This recipe is arrow 59 / DataFusion 55.1, which matches the default in
[base](base.md) except that the server needs `datafusion ^55.1`, not 55.0.

| datafusion-flight-sql-server | needs datafusion | needs arrow |
|-----------------------------:|-----------------:|------------:|
|      0.4.19 (this recipe)    |           ^55.1  |          59 |
|                       0.4.18 |           ^54.0  |          58 |
|                       0.4.16 |           ^53.0  |          58 |

If you must stay on 0.4.18 (DataFusion 54), also pin
`datafusion-federation@=0.5.5`: 0.5.6 raised its requirement to
`datafusion ^55` in a patch release, and cargo would otherwise resolve both
DataFusion 54 and 55 into one graph. Either way, check for a single version
before debugging type errors:

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

Copy the cookbook's fixture into `./data` and start the server:

```console
$ mkdir -p data && cp <cookbook>/testing/data/trips.parquet data/
$ cargo run
registered trips
serving 1 table(s) on 127.0.0.1:50051
```

Use the debug profile to verify. A release build takes roughly twice as long
and the server answers queries identically either way.

From another terminal, query it with the client from
[flight-sql-client](flight-sql-client.md):

```console
$ flight_sql_client --host 127.0.0.1 --port 50051 \
    statement-query "select count(*) as n from trips"
+---+
| n |
+---+
| 6 |
+---+
$ flight_sql_client --host 127.0.0.1 --port 50051 \
    statement-query "select passenger_count, count(*) as trips from trips where passenger_count > 0 group by passenger_count order by passenger_count"
+-----------------+-------+
| passenger_count | trips |
+-----------------+-------+
| 1               | 2     |
| 2               | 3     |
+-----------------+-------+
```

Expected: exactly the tables above. See
[testing/data](../testing/data/README.md) for the fixture's contents.

## Notes

- `FlightSqlService::new` takes the `SessionState` (`ctx.state()`), not the
  `SessionContext`.
- The service speaks gRPC over HTTP/2. Behind a proxy that only handles
  HTTP/1.1 the connection fails in a way that reads like an auth error.
- Clients need not share this project's arrow version — see
  [flight-sql-client](flight-sql-client.md).
