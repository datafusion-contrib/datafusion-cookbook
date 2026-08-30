# Testing

We evaluate the prompts and information in this repository in the following
way.

## Goal

Identify any improvements to this repository, or to upstream repositories, that
would make it easier and faster to build the systems described here.

The output of a run is not the built system. It is the **list of things that
slowed the agent down** — those become issues, pitfalls in ingredient files, or
upstream patches.

## Metrics

**Time to Awesome (TM)**: how quickly a system is built and working, in a way
that is awesome and satisfies the example queries of the prompt.

> Attribution: Andrew's original note credits this to Paul Dix with a `TODO
> find link`. A search did not turn up a citable source, so it is left open
> rather than guessed at. If you know the reference, please add it.

Specifically:

1. How fast (wallclock time) can the agent produce the first version that
   satisfies the prompt?
2. How many tokens, or other measure of effort, does it take?
3. How much back and forth does it require?
4. How well does the result satisfy the prompt — all of the acceptance
   criteria, or only some?

Metric 4 is the one to record carefully. Every prompt in
[PROMPTS.md](PROMPTS.md) has numbered acceptance criteria; report the score as
`criteria met / total` so runs are comparable.

## Methodology

1. Use the specified coding tool and a prompt from
   [PROMPTS.md](PROMPTS.md), verbatim.
2. Record what the agent was given besides the prompt — nothing, `llms.txt`,
   the whole repo. **This is the independent variable.** A run that does not
   record it cannot be compared against another.
3. Evaluate the built system against the acceptance criteria.
4. Note every point where the agent stalled, guessed wrong, or needed
   correction.
5. File issues or pull requests against this repo or the upstream
   repositories. A wrong turn that is now a documented pitfall is the unit of
   progress here.

### On what to vary

The open question behind this repo — see
[issue #5](https://github.com/datafusion-contrib/datafusion-cookbook/issues/5)
— is which format actually helps an agent: a menu of tables, front-mattered
ingredient files, an `llms.txt` index, `AGENTS.md`, skills, or something else.

Rather than settling that by argument, vary the **context** column below and
hold the prompt and the tool fixed. That turns the format question into a
measurement. The current repo layout is designed for this: ingredients are
authored once and rendered into multiple formats, so a new format is a
renderer rather than a rewrite.

## Current Results

| Agent | Prompt | Context given | Speed to initial completion | Cost of initial completion | Required back and forth | Criteria met | Notes |
|-------|--------|---------------|------------------------------|----------------------------|-------------------------|--------------|-------|
| Claude Opus 4.5 (Claude Code) | DuckDB style CLI | Whole repo, written by the same session | Not measured | Not measured | 0 human turns | 7/7 | Produced [recipes/duckdb-style-cli](recipes/duckdb-style-cli/) |

This row is not a clean measurement. The agent wrote the cookbook in the same
session, so it had the pitfalls in working memory rather than merely on disk,
and neither wallclock nor token cost was instrumented — only the criteria-met
column is real. It establishes that the prompt is answerable and its criteria
are checkable, nothing more.

The comparison that matters has not been run: the same prompt, cold sessions,
varying only the context given.

### Known blockers, before any run

The first things a run is expected to hit. If a run does *not* hit them, that
is itself worth recording.

1. **DataFusion 54 vs 55.** DataFusion 55 is the newest release, but nearly all
   of the ecosystem still requires `^54`. An agent that reaches for the latest
   version and then adds a connector gets two incompatible copies of
   DataFusion in one dependency graph.
2. **`parquet_scan()` does not exist.** The DataFusion equivalent is
   `enable_url_table()` plus `SELECT * FROM 'file.parquet'`.
3. **Redundant table formatters.** DataFusion re-exports Arrow's pretty
   printer; adding `comfy-table` on top is wasted work.
4. **Explicit registration.** `datafusion-functions-json` and
   `datafusion-tracing` both do nothing until registered on the context. The
   failure looks unrelated to the missing registration call.
