# datafusion-cookbook

This is an Agent first cookbook. 

It contains recipes for quickly assembling custom analytic systems from Apache DataFusion

(TODO links to the datafusion website and documentation and paper)

You can choose from a menu of options to assemble a custom analytic system that
meets your needs.

Example prompts:
- "make a duckdb style CLI that uses Apache 2 licensed code with nice display formatting that can read parquet and json"

See more prompts in the [PROMPTS.md](PROMPTS.md) file.

Example XXX:


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
cargo add datafusion@55.0.0
```

## Additional Ingredients

### REPL Scaffolding


| Format | Description      | Required Dependency | Install / Usage Instructions |   |   |
|--------|:-----------------|---------------------|------------------------------|---|---|
| TODO   | Basic CLI / REPL |                     | TODO                         |   |   |


### Data Format Support

| Format  | Description                          | Required Dependency                   | Install / Usage Instructions |   |   |
|---------|:-------------------------------------|---------------------------------------|------------------------------|---|---|
| Variant | Fast binary semi-structured support  | datafusion-contrib/datafusion-variant | TODO                         |   |   |

## Data Generation

| Format       | Description                                    | Required Dependency                   | Install / Usage Instructions |   |   |
|--------------|:-----------------------------------------------|---------------------------------------|------------------------------|---|---|
| TPC-H/TPC-DS | Fast generation of standard benchmark datasets | https://github.com/datafusion-contrib/tpcgen-rs | TODO                         |   |   |


### File Formats

| Format  | Description | Required Dependency          | Install / Usage Instructions |   |   |
|---------|:------------|------------------------------|------------------------------|---|---|
| Parquet |             | None (built with DataFusion) |                              |   |   |
| JSON    |             | None (built with DataFusion) |                              |   |   |
| Vortex  |             | TODO                         |                              |   |   |
| Zarr    |             | TODO                         |                              |   |   |

## Observability

| Format | Description             | Required Dependency                   | Install / Usage Instructions |   |   |
|--------|:------------------------|---------------------------------------|------------------------------|---|---|
| otel   | Open Telemetry Support  | datafusion-contrib/datafusion-tracing | TODO                         |   |   |

## Connectors

| Format   | Description                      | Required Dependency                           | Install / Usage Instructions |   |   |
|----------|:---------------------------------|-----------------------------------------------|------------------------------|---|---|
| postgres | Connect to a PostgreSQL database | datafusion-contrib/datafusion-table_providers | TODO                         |   |   |

## Wire Transport

| Format       | Description             | Required Dependency | Install / Usage Instructions |   |   |
|--------------|:------------------------|---------------------|------------------------------|---|---|
| arrow-flight | Arrow native transpport | arrow-flight        | TODO                         |   |   |
| Flight SQL   | Arrow native SQL        | arrow-flight        | TODO                         |   |   |
| ADBC         | TODO                    |                | TODO                         |   |   |
| postgres     | Postgres wire protocol  | TODO                | TODO                         |   |   |




Future items to add to the menu:



# Instructions for adding a new recipe:
TODO