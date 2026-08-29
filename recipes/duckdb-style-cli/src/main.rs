//! yadb — a DuckDB-style SQL CLI built on Apache DataFusion.
//!
//! Recipe for the prompt in PROMPTS.md:
//!
//! > Make a DuckDB style CLI using Apache 2 licensed code, with nice display
//! > formatting, that can read Parquet and JSON files.
//!
//! Ingredients used: datafusion-core, rustyline, pretty-printing, parquet, json.

use std::time::Instant;

use datafusion::arrow::util::pretty::pretty_format_batches;
// Not in the prelude, and note the name: NdJsonReadOptions is a deprecated
// alias for this type.
use datafusion::execution::options::JsonReadOptions;
use datafusion::prelude::*;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

/// Files given on the command line are registered as tables named after the
/// file stem, so `yadb trips.parquet` makes `SELECT * FROM trips` work.
///
/// Files can also be queried by path without registering anything, because
/// the context below is built with `enable_url_table`.
struct Args {
    files: Vec<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut files = Vec::new();

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "-V" | "--version" => {
                println!("yadb {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown option: {other}"));
            }
            other => files.push(other.to_string()),
        }
    }

    Ok(Args { files })
}

fn print_help() {
    println!(
        "\
yadb {} — a DuckDB-style SQL CLI built on Apache DataFusion

USAGE:
    yadb [FILES]...

ARGS:
    <FILES>...    Parquet (.parquet) or newline-delimited JSON (.ndjson, .jsonl,
                  .json) files to register as tables, named after the file stem.

OPTIONS:
    -h, --help       Print this help
    -V, --version    Print version

Files can also be queried directly by path, without registering them first:

    yadb> SELECT * FROM 'data/trips.parquet' LIMIT 5;

Commands:
    .tables      List registered tables
    .schema T    Show the schema of table T
    .help        Show this help
    .quit        Exit (Ctrl-D also works)
",
        env!("CARGO_PKG_VERSION")
    );
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("yadb: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;

    // Two opt-ins that a REPL needs and that are off by default:
    //
    // `with_information_schema` backs SHOW TABLES and DESCRIBE. Without it
    // those fail with "SHOW TABLES is not supported unless information_schema
    // is enabled", which reads like a missing feature rather than a config
    // flag.
    //
    // `enable_url_table` is what makes `SELECT * FROM 'file.parquet'` work.
    // DataFusion has no `parquet_scan()` function; without this call that
    // query fails with a table-not-found error.
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config).enable_url_table();

    for path in &args.files {
        register_file(&ctx, path).await?;
    }

    repl(&ctx).await
}

/// Register one file as a table named after its stem, choosing the reader
/// from the extension.
async fn register_file(ctx: &SessionContext, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let stem = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("cannot derive a table name from '{path}'"))?
        .to_string();

    let extension = std::path::Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    match extension.as_str() {
        "parquet" => {
            ctx.register_parquet(&stem, path, ParquetReadOptions::default())
                .await?;
        }
        "ndjson" | "jsonl" | "json" => {
            // DataFusion reads newline-delimited JSON. A file holding a single
            // JSON array will not parse, so say so plainly rather than letting
            // an arrow error surface.
            // `file_extension` borrows, so the string needs to outlive the
            // options value rather than being a temporary.
            let dotted = format!(".{extension}");
            let options = JsonReadOptions::default().file_extension(&dotted);
            ctx.register_json(&stem, path, options).await.map_err(|e| {
                format!(
                    "{path}: {e}\n\
                     hint: JSON input must be newline-delimited (one object per line), \
                     not a single JSON array"
                )
            })?;
        }
        "" => return Err(format!("{path}: no file extension; cannot pick a reader").into()),
        other => {
            return Err(format!(
                "{path}: unsupported extension '.{other}' (expected .parquet, .ndjson, .jsonl or .json)"
            )
            .into())
        }
    }

    println!("Registered {path} as table \"{stem}\"");
    Ok(())
}

async fn repl(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    let mut editor = DefaultEditor::new()?;
    let mut buffer = String::new();

    println!(
        "yadb {} — DataFusion {}. Type .help for help, Ctrl-D to exit.",
        env!("CARGO_PKG_VERSION"),
        datafusion_version()
    );

    loop {
        // A continuation prompt makes it obvious that the statement is still
        // open and waiting for its terminating semicolon.
        let prompt = if buffer.is_empty() {
            "yadb> "
        } else {
            "  ...> "
        };

        match editor.readline(prompt) {
            Ok(line) => {
                if buffer.is_empty() && line.trim().starts_with('.') {
                    if handle_dot_command(ctx, line.trim()).await {
                        break;
                    }
                    let _ = editor.add_history_entry(line.trim());
                    continue;
                }

                buffer.push_str(&line);
                buffer.push('\n');

                // SQL is multi-line; keep accumulating until the statement is
                // terminated. `readline` hands back one line at a time.
                if !buffer.trim_end().ends_with(';') {
                    continue;
                }

                let sql = buffer.trim().trim_end_matches(';').trim().to_string();
                buffer.clear();

                if sql.is_empty() {
                    continue;
                }

                let _ = editor.add_history_entry(sql.as_str());

                // A failed query must return to the prompt, not end the
                // session — this is the whole point of matching here rather
                // than using `?`.
                if let Err(err) = execute(ctx, &sql).await {
                    eprintln!("Error: {err}");
                }
            }

            // Ctrl-C abandons the half-typed statement but keeps the session.
            Err(ReadlineError::Interrupted) => {
                if buffer.is_empty() {
                    println!("(use Ctrl-D or .quit to exit)");
                } else {
                    buffer.clear();
                    println!("(statement cancelled)");
                }
            }

            // Ctrl-D exits.
            Err(ReadlineError::Eof) => break,

            Err(err) => return Err(err.into()),
        }
    }

    Ok(())
}

async fn execute(ctx: &SessionContext, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();

    let batches = ctx.sql(sql).await?.collect().await?;
    let elapsed = started.elapsed();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if rows == 0 {
        println!("(0 rows, {:.3}s)", elapsed.as_secs_f64());
        return Ok(());
    }

    println!("{}", pretty_format_batches(&batches)?);
    println!(
        "({} row{}, {:.3}s)",
        rows,
        if rows == 1 { "" } else { "s" },
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Returns true when the session should end.
async fn handle_dot_command(ctx: &SessionContext, line: &str) -> bool {
    let mut parts = line.splitn(2, char::is_whitespace);
    let command = parts.next().unwrap_or_default();
    let argument = parts.next().map(str::trim).unwrap_or_default();

    match command {
        ".quit" | ".exit" => return true,
        ".help" => print_help(),
        // SHOW TABLES also lists the information_schema views, which is noise
        // in a REPL. Query the catalog directly and keep the user's tables.
        ".tables" => match ctx
            .sql(
                "SELECT table_name, table_type FROM information_schema.tables \
                 WHERE table_schema NOT IN ('information_schema') \
                 ORDER BY table_name",
            )
            .await
        {
            Ok(df) => match df.collect().await {
                Ok(batches) => match pretty_format_batches(&batches) {
                    Ok(table) => println!("{table}"),
                    Err(err) => eprintln!("Error: {err}"),
                },
                Err(err) => eprintln!("Error: {err}"),
            },
            Err(err) => eprintln!("Error: {err}"),
        },
        ".schema" => {
            if argument.is_empty() {
                eprintln!("Error: .schema needs a table name");
            } else if let Err(err) = execute(ctx, &format!("DESCRIBE {argument}")).await {
                eprintln!("Error: {err}");
            }
        }
        other => eprintln!("Error: unknown command '{other}' (try .help)"),
    }

    false
}

fn datafusion_version() -> &'static str {
    // Reported by DataFusion itself rather than hardcoded, so the banner
    // cannot drift from the dependency actually linked.
    datafusion::DATAFUSION_VERSION
}
