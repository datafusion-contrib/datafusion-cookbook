#!/usr/bin/env python3
"""Regenerate the recipe and prompt index tables from file frontmatter.

The frontmatter (`--- ... ---` block) at the top of each recipe and
prompt is the source of truth. The tables between
`<!-- BEGIN GENERATED: ... -->` / `<!-- END GENERATED: ... -->` markers
in README.md, prompts/README.md, and future-recipes/README.md are
rewritten by this script — edit the frontmatter, not the tables.

Run from anywhere:

    python3 scripts/regen_index.py
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# category slug -> display name, in index sort order
CATEGORIES = {
    "base": "Base",
    "repl-cli": "REPL / CLI",
    "file-formats": "File Formats",
    "semi-structured-data": "Semi-structured Data",
    "data-generation": "Data Generation",
    "interop": "Interop",
    "connectors": "Connectors",
    "wire-transport": "Wire Transport",
    "observability": "Observability",
}
STATUSES = ("verified", "draft", "blocked")


def parse_frontmatter(path):
    lines = path.read_text().splitlines()
    if not lines or lines[0].strip() != "---":
        sys.exit(f"{path}: missing frontmatter (file must start with ---)")
    meta = {}
    for lineno, line in enumerate(lines[1:], start=2):
        if line.strip() == "---":
            return meta
        m = re.match(r"^([A-Za-z_]+):\s*(.*)$", line)
        if not m:
            sys.exit(f"{path}:{lineno}: unparseable frontmatter line: {line!r}")
        key, value = m.group(1), m.group(2).strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        elif value.startswith("[") and value.endswith("]"):
            value = [v.strip() for v in value[1:-1].split(",") if v.strip()]
        meta[key] = value
    sys.exit(f"{path}: unterminated frontmatter (no closing ---)")


def collect(dirname, required):
    entries = []
    for path in sorted((ROOT / dirname).glob("*.md")):
        if path.name in ("README.md", "TEMPLATE.md"):
            continue
        meta = parse_frontmatter(path)
        for key in required:
            if key not in meta:
                sys.exit(f"{path}: frontmatter missing required key {key!r}")
        if "category" in meta and meta["category"] not in CATEGORIES:
            sys.exit(
                f"{path}: unknown category {meta['category']!r}"
                f" (known: {', '.join(CATEGORIES)})"
            )
        if "status" in meta and meta["status"] not in STATUSES:
            sys.exit(
                f"{path}: unknown status {meta['status']!r}"
                f" (known: {', '.join(STATUSES)})"
            )
        meta["file"] = path.name
        entries.append(meta)
    return entries


def recipe_sort_key(meta):
    return (list(CATEGORIES).index(meta["category"]), meta["name"])


def table(header, rows):
    lines = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join("-" * (len(h) + 2) for h in header) + "|",
    ]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def recipe_table(recipes, link_prefix):
    rows = [
        (
            f"[{r['name']}]({link_prefix}{r['file']})",
            CATEGORIES[r["category"]],
            r["provides"],
            "✅" if r["status"] == "verified" else "TODO",
        )
        for r in sorted(recipes, key=recipe_sort_key)
    ]
    return table(("Recipe", "Category", "Provides", "Verified"), rows)


def future_recipe_table(recipes, link_prefix):
    rows = [
        (
            f"[{r['name']}]({link_prefix}{r['file']})",
            CATEGORIES[r["category"]],
            r["provides"],
            r.get("status_note", r["status"]),
        )
        for r in sorted(recipes, key=recipe_sort_key)
    ]
    return table(("Recipe", "Category", "Provides", "Status"), rows)


def prompt_table(prompts, link_prefix):
    rows = [
        (
            f"[{p['name']}]({link_prefix}{p['file']})",
            p["builds"],
            ", ".join(p["recipes"]),
        )
        for p in sorted(prompts, key=lambda p: p["name"])
    ]
    return table(("Prompt", "Builds", "Uses Recipes"), rows)


def splice(relpath, block_name, content):
    path = ROOT / relpath
    begin = f"<!-- BEGIN GENERATED: {block_name} -->"
    end = f"<!-- END GENERATED: {block_name} -->"
    text = path.read_text()
    if begin not in text or end not in text:
        sys.exit(f"{relpath}: missing {begin} / {end} markers")
    pre, rest = text.split(begin, 1)
    _, post = rest.split(end, 1)
    new = pre + begin + "\n" + content + "\n" + end + post
    if new != text:
        path.write_text(new)
        print(f"updated   {relpath}")
    else:
        print(f"unchanged {relpath}")


def main():
    recipes = collect("recipes", ("name", "category", "provides", "status"))
    future = collect("future-recipes", ("name", "category", "provides", "status"))
    prompts = collect("prompts", ("name", "builds", "recipes"))

    known = {r["name"] for r in recipes + future}
    for p in prompts:
        for name in p["recipes"]:
            if name not in known:
                sys.exit(f"prompts/{p['file']}: unknown recipe {name!r} in frontmatter")

    splice("README.md", "prompt-index", prompt_table(prompts, "prompts/"))
    splice("README.md", "recipe-index", recipe_table(recipes, "recipes/"))
    splice("prompts/README.md", "prompt-index", prompt_table(prompts, ""))
    splice(
        "future-recipes/README.md",
        "future-recipe-index",
        future_recipe_table(future, ""),
    )


if __name__ == "__main__":
    main()
