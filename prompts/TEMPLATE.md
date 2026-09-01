<!--
This file contains the structure that new prompts should follow

The prompt index tables in [README.md](../README.md) and
[README.md](README.md) are generated from the frontmatter below by
`python3 scripts/regen_index.py`; run that script instead of editing
the tables by hand.

Note that the prompt BODY purposely DOES NOT include links to the
recipes, to ensure that the recipes are discoverable on their own; the
recipes the prompt exercises are recorded only in the `recipes:`
frontmatter field.
-->

---
name: <kebab-case-slug>
builds: <one line shown in the prompt index>
recipes: [base, <recipe>, ...]
---

# <System name>

The prompt itself: a short description of the system to build, written
as you would give it to a coding agent.

## Example usage

How a user invokes the finished system (command line, SQL, etc).

## Example output

What the finished system should print for the example usage.

## Features

Numbered list of required features.

## Additional potential options

Optional follow-on features to try after the base system works.


