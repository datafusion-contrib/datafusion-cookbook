#!/usr/bin/env python3
"""Render the menu in README.md and the llms.txt index from ingredients/*.md.

The ingredient files are the source of truth. This script rewrites the region
between the GENERATED markers in README.md, and rewrites llms.txt entirely.

Usage:
    python3 dev/generate.py            # rewrite README.md and llms.txt
    python3 dev/generate.py --check    # exit 1 if they are out of date

Each ingredient is Markdown with a YAML front-matter header:

    ---
    name: postgres-connector        # required, must match the filename
    title: PostgreSQL               # required, menu row label
    category: connectors            # required, one of CATEGORIES below
    summary: One line.              # required, becomes the menu row
    when_to_use: One sentence.      # required
    datafusion: "54"                # required, compatible major, or "any"
    status: stable                  # required: stable|experimental|unpublished
    crate: datafusion-table-providers    # omit for built-ins
    version: "0.13.1"                    # quote it, so 54.0 stays a string
    features: [postgres]                 # required cargo features
    arrow: "58"                          # for crates pinned to arrow, not DF
    license: Apache-2.0                  # SPDX
    repo: https://github.com/...
    install: cargo add datafusion-table-providers@0.13.1 --features postgres
    pitfalls:
      - One line each.
    example: https://example.com/working/code
    ---

    Prose and a snippet that compiles.

Deliberately dependency-free: it runs with a bare python3 and no virtualenv,
because a generator that needs its own setup step does not get run.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INGREDIENTS = ROOT / "ingredients"
README = ROOT / "README.md"
LLMS = ROOT / "llms.txt"

BEGIN = "<!-- BEGIN GENERATED MENU -->"
END = "<!-- END GENERATED MENU -->"

# Ordered. Each entry is (category key, heading, blurb).
CATEGORIES = [
    ("base", "Base", "Start here. Every system needs this."),
    ("repl", "REPL Scaffolding", "Turning the engine into something a human can type at."),
    ("file-formats", "File Formats", "What you can read and write."),
    ("data-generation", "Data Generation", "Test data, when you have none."),
    ("observability", "Observability", "Seeing what the engine is doing."),
    ("connectors", "Connectors", "Querying systems that are not files."),
    ("wire-transport", "Wire Transport", "Letting other processes query you."),
]

REQUIRED = ("name", "title", "category", "summary", "when_to_use", "status", "datafusion")


class IngredientError(Exception):
    pass


def parse_front_matter(text: str, path: Path) -> tuple[dict, str]:
    """Parse the YAML subset used by ingredient files.

    Supports `key: value`, `key: [a, b]`, and block lists of `- item`.
    Values may be quoted. This is not a general YAML parser; it covers exactly
    what the ingredient format documents, and raises on anything else so that
    a malformed file fails loudly instead of silently losing a field.
    """
    if not text.startswith("---\n"):
        raise IngredientError(f"{path.name}: missing front matter")

    end = text.find("\n---\n", 4)
    if end == -1:
        raise IngredientError(f"{path.name}: unterminated front matter")

    header, body = text[4:end], text[end + 5 :]
    data: dict[str, object] = {}
    current_list_key: str | None = None

    for lineno, raw in enumerate(header.split("\n"), start=2):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue

        if raw.startswith((" ", "\t")) or raw.lstrip().startswith("- "):
            item = raw.strip()
            if not item.startswith("- "):
                raise IngredientError(f"{path.name}:{lineno}: expected a '- ' list item")
            if current_list_key is None:
                raise IngredientError(f"{path.name}:{lineno}: list item outside a key")
            data[current_list_key].append(unquote(item[2:].strip()))  # type: ignore[union-attr]
            continue

        if ":" not in raw:
            raise IngredientError(f"{path.name}:{lineno}: expected 'key: value'")

        key, _, value = raw.partition(":")
        key, value = key.strip(), value.strip()

        if not value:
            data[key] = []
            current_list_key = key
        elif value.startswith("[") and value.endswith("]"):
            inner = value[1:-1].strip()
            data[key] = [unquote(v.strip()) for v in inner.split(",") if v.strip()]
            current_list_key = None
        else:
            data[key] = unquote(value)
            current_list_key = None

    return data, body


def unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def load_ingredients() -> list[dict]:
    ingredients = []
    known = {key for key, _, _ in CATEGORIES}

    for path in sorted(INGREDIENTS.glob("*.md")):
        if path.name == "README.md":
            continue

        data, _ = parse_front_matter(path.read_text(), path)
        data["_file"] = f"ingredients/{path.name}"

        missing = [f for f in REQUIRED if f not in data]
        if missing:
            raise IngredientError(f"{path.name}: missing required field(s): {', '.join(missing)}")
        if data["name"] != path.stem:
            raise IngredientError(f"{path.name}: name '{data['name']}' does not match filename")
        if data["category"] not in known:
            raise IngredientError(
                f"{path.name}: unknown category '{data['category']}'. "
                f"Known: {', '.join(sorted(known))}"
            )
        ingredients.append(data)

    return ingredients


def dependency_cell(ing: dict) -> str:
    crate = ing.get("crate")
    if not crate:
        # An unpublished ingredient has no crates.io name, but it is not
        # dependency-free either — saying "built in" would be wrong.
        if ing.get("status") == "unpublished":
            return f"git: {ing.get('repo', 'see ingredient')}"
        return "None (built in)"

    version = ing.get("version")
    cell = f"`{crate}"
    if version:
        cell += f"@{version}"
    cell += "`"

    features = ing.get("features")
    if features:
        cell += " + `" + ",".join(features) + "`"
    return cell


def status_cell(ing: dict) -> str:
    status = ing.get("status", "")
    return {
        "stable": "stable",
        "experimental": "⚠️ experimental",
        "unpublished": "⚠️ unpublished",
    }.get(status, status)


def render_menu(ingredients: list[dict]) -> str:
    out: list[str] = []

    for key, heading, blurb in CATEGORIES:
        rows = [i for i in ingredients if i["category"] == key]
        if not rows:
            continue

        rows.sort(key=lambda i: (i.get("status") != "stable", i["title"]))

        out.append(f"### {heading}")
        out.append("")
        out.append(blurb)
        out.append("")
        out.append("| Ingredient | Description | Dependency | DF | Status |")
        out.append("|------------|-------------|------------|----|--------|")
        for ing in rows:
            out.append(
                "| [{title}]({file}) | {summary} | {dep} | {df} | {status} |".format(
                    title=ing["title"],
                    file=ing["_file"],
                    summary=ing["summary"],
                    dep=dependency_cell(ing),
                    df=ing.get("datafusion", ""),
                    status=status_cell(ing),
                )
            )
        out.append("")

    return "\n".join(out).rstrip() + "\n"


def render_llms(ingredients: list[dict]) -> str:
    out = [
        "# DataFusion Cookbook",
        "",
        "> Ingredients for assembling custom analytic systems from Apache DataFusion.",
        "> Each ingredient is a building block with a verified crate version, an",
        "> install command, and its known pitfalls. Generated from ingredients/*.md",
        "> by dev/generate.py — do not edit by hand.",
        "",
        "Unless stated otherwise, every ingredient here is pinned against DataFusion 54.",
        "DataFusion 55 exists but most of the ecosystem has not moved to it yet; mixing",
        "majors puts two incompatible copies of DataFusion in one dependency graph.",
        "",
    ]

    for key, heading, _ in CATEGORIES:
        rows = [i for i in ingredients if i["category"] == key]
        if not rows:
            continue

        out.append(f"## {heading}")
        out.append("")
        for ing in sorted(rows, key=lambda i: i["title"]):
            out.append(f"- [{ing['title']}]({ing['_file']}): {ing['summary']}")
            out.append(f"  - When to use: {ing['when_to_use']}")
            if ing.get("install"):
                out.append(f"  - Install: `{ing['install']}`")
            out.append(f"  - DataFusion: {ing.get('datafusion')} | Status: {ing.get('status')}")
            for pitfall in ing.get("pitfalls", []) or []:
                out.append(f"  - Pitfall: {pitfall}")
        out.append("")

    out.append("## Prompts")
    out.append("")
    out.append("- [PROMPTS.md](PROMPTS.md): Example systems to build, with acceptance criteria.")
    out.append("- [TESTING.md](TESTING.md): How we evaluate whether this repo actually helps.")
    out.append("")

    return "\n".join(out)


def splice_readme(menu: str) -> str:
    text = README.read_text()
    start, end = text.find(BEGIN), text.find(END)

    if start == -1 or end == -1:
        raise IngredientError(
            f"README.md is missing the {BEGIN} / {END} markers; cannot place the generated menu."
        )
    if end < start:
        raise IngredientError("README.md has the generated markers in the wrong order.")

    return text[: start + len(BEGIN)] + "\n\n" + menu + "\n" + text[end:]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the generated files are current; do not write",
    )
    args = parser.parse_args()

    try:
        ingredients = load_ingredients()
        readme = splice_readme(render_menu(ingredients))
        llms = render_llms(ingredients)
    except IngredientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.check:
        stale = [
            name
            for name, path, new in (("README.md", README, readme), ("llms.txt", LLMS, llms))
            if not path.exists() or path.read_text() != new
        ]
        if stale:
            print(
                "error: out of date: " + ", ".join(stale) + "\nRun: python3 dev/generate.py",
                file=sys.stderr,
            )
            return 1
        print(f"ok: README.md and llms.txt match {len(ingredients)} ingredients")
        return 0

    README.write_text(readme)
    LLMS.write_text(llms)
    print(f"wrote README.md and llms.txt from {len(ingredients)} ingredients")
    return 0


if __name__ == "__main__":
    sys.exit(main())
