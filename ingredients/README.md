# Ingredients

Each file in this directory describes one **ingredient** — a reusable building
block you can add to a DataFusion-based system.

An ingredient file is the *single source of truth*. The menu tables in the
top-level [README.md](../README.md) and the [llms.txt](../llms.txt) index are
both generated from these files by [`dev/generate.py`](../dev/generate.py). Do
not hand-edit the generated sections; edit the ingredient and regenerate.

## Format

Each file is Markdown with a YAML front-matter header:

```markdown
---
name: postgres
title: PostgreSQL Connector
category: connectors
summary: One line. Shows up in the menu table.
when_to_use: One sentence. The condition under which an agent should reach for this.
crate: datafusion-table-providers
version: "0.13.1"
features: [postgres]
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-table-providers
install: cargo add datafusion-table-providers@0.13.1 --features postgres
status: stable
pitfalls:
  - Each pitfall is one line.
---

Prose and a code sample go here.
```

### Fields

| Field | Required | Meaning |
|-------|----------|---------|
| `name` | yes | Stable slug. Must match the filename. |
| `title` | yes | Human-readable name, used as the menu row label. |
| `category` | yes | One of the categories in `dev/generate.py`. Groups rows into menu tables. |
| `summary` | yes | One line describing what it is. |
| `when_to_use` | yes | The condition that should make an agent select this ingredient. |
| `crate` | no | Published crate name. Omit for built-ins. |
| `version` | no | Exact version this cookbook is tested against. Quote it, so `54.0` does not become a float. |
| `features` | no | Cargo features required. |
| `datafusion` | yes | Major version of `datafusion` this is compatible with, or `any` for things that do not link DataFusion. |
| `license` | no | SPDX identifier. Relevant because some prompts require Apache-2.0. |
| `repo` | no | Source repository. |
| `install` | no | The literal shell command to add it. |
| `status` | yes | `stable`, `experimental`, or `unpublished`. |
| `pitfalls` | no | List of known gotchas. This is the highest-value field — it is what stops an agent burning a cycle on a known problem. |
| `example` | no | Link to working example code. |

### Why this shape

Issue [#5](https://github.com/datafusion-contrib/datafusion-cookbook/issues/5)
weighs several conventions for agent-consumable Markdown (`llms.txt`,
`AGENTS.md`, `SKILL.md`, and the spec-driven-development frameworks). Rather
than adopt one, ingredients are authored once in this neutral front-matter and
the other formats are *rendered* from it. If a convention wins, we add a
renderer; we do not rewrite the content.

## Adding an ingredient

1. Copy an existing file in this directory.
2. Fill in the front matter. Verify `version` and `datafusion` against
   crates.io — do not guess. `dev/check_versions.py` will check them for you.
3. Write a body with a snippet that actually compiles.
4. Run `python3 dev/generate.py` to regenerate `README.md` and `llms.txt`.
