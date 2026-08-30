# Ingredients

One file per ingredient. These are the source of truth: the menu in
[../README.md](../README.md) and [../llms.txt](../llms.txt) are generated from
them by `dev/generate.py`, so edit the ingredient and regenerate rather than
editing the generated output.

```markdown
---
name: postgres-connector
title: PostgreSQL
category: connectors
summary: One line. Becomes the menu row.
when_to_use: The condition that should make an agent pick this.
crate: datafusion-table-providers
version: "0.13.1"
features: [postgres]
datafusion: "54"
license: Apache-2.0
repo: https://github.com/datafusion-contrib/datafusion-table-providers
install: cargo add datafusion-table-providers@0.13.1 --features postgres
status: stable
pitfalls:
  - One line each.
example: https://example.com/working/code
---

Prose and a snippet that compiles.
```

| Field | Required | Notes |
|-------|----------|-------|
| `name` | yes | Must match the filename |
| `title` | yes | Menu row label |
| `category` | yes | One of the categories in `dev/generate.py` |
| `summary` | yes | One line |
| `when_to_use` | yes | One sentence |
| `datafusion` | yes | Compatible major, or `any` |
| `status` | yes | `stable`, `experimental`, or `unpublished` |
| `crate` | no | Omit for built-ins |
| `version` | no | Quote it, so `54.0` stays a string |
| `features` | no | Required cargo features |
| `arrow` | no | For crates pinned to the arrow train rather than DataFusion |
| `license` | no | SPDX. Some prompts require Apache-2.0 |
| `repo` | no | Source repository |
| `install` | no | Literal shell command |
| `pitfalls` | no | The highest-value field — what stops an agent losing a cycle |
| `example` | no | Link to working code |

Verify `version` and `datafusion` against crates.io with
`python3 dev/check_versions.py <name>` rather than guessing. CI runs it, and
`dev/generate.py --check`, on every push.
