#!/usr/bin/env python3
"""Check ingredient versions against crates.io.

Two things are verified for every ingredient that names a crate:

  1. the pinned version exists on crates.io (catches typos and inventions);
  2. the DataFusion major it requires matches the ingredient's `datafusion`
     field (catches the version skew that is currently the single biggest
     source of build failure in the DataFusion ecosystem).

Reads the sparse index rather than the crates.io JSON API, which has no rate
limit and needs no credentials.

Usage:
    python3 dev/check_versions.py            # check every ingredient
    python3 dev/check_versions.py postgres-connector tracing
"""

from __future__ import annotations

import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate import IngredientError, load_ingredients  # noqa: E402

UA = {"User-Agent": "datafusion-cookbook version check"}


def index_path(name: str) -> str:
    n = len(name)
    if n <= 2:
        return f"{n}/{name}"
    if n == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def fetch(crate: str) -> list[dict]:
    url = "https://index.crates.io/" + index_path(crate.lower())
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return [json.loads(line) for line in resp.read().decode().splitlines() if line.strip()]


def major_of(req: str) -> str | None:
    """Extract the major version from a cargo requirement such as '^54.0'."""
    match = re.search(r"(\d+)", req)
    return match.group(1) if match else None


def arrow_major(row: dict, crate: str) -> str | None:
    """The arrow major a crate is built against.

    Some ingredients are pinned to the arrow release train rather than to
    DataFusion — arrow-flight is the notable one. For those, arrow is the
    constraint that actually decides whether the build resolves.
    """
    for dep in row["deps"]:
        if dep["kind"] != "normal":
            continue
        if dep["name"] == "arrow" or dep["name"].startswith("arrow-"):
            return major_of(dep["req"])

    # arrow-rs crates are versioned in lockstep with arrow itself.
    if crate == "arrow" or crate.startswith("arrow-"):
        return major_of(row["vers"])

    return None


def datafusion_major(row: dict) -> str | None:
    """The DataFusion major a crate requires.

    Checks the `datafusion` facade first, then any `datafusion-*` subcrate,
    since crates like vortex-datafusion depend on the pieces rather than the
    facade.
    """
    deps = [d for d in row["deps"] if d["kind"] == "normal"]

    for dep in deps:
        if dep["name"] == "datafusion":
            return major_of(dep["req"])

    for dep in deps:
        if dep["name"].startswith("datafusion-"):
            return major_of(dep["req"])

    return None


def main() -> int:
    try:
        ingredients = load_ingredients()
    except IngredientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    wanted = set(sys.argv[1:])
    if wanted:
        ingredients = [i for i in ingredients if i["name"] in wanted]
        missing = wanted - {i["name"] for i in ingredients}
        if missing:
            print(f"error: no such ingredient(s): {', '.join(sorted(missing))}", file=sys.stderr)
            return 2

    problems = 0
    checked = 0
    skipped = []

    for ing in ingredients:
        crate, version = ing.get("crate"), ing.get("version")
        name = ing["name"]

        if ing.get("status") == "unpublished":
            skipped.append(f"{name} (unpublished; not on crates.io)")
            continue
        if not crate:
            skipped.append(f"{name} (no crate; built in)")
            continue

        try:
            rows = fetch(crate)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                print(f"FAIL {name}: crate '{crate}' not found on crates.io")
                problems += 1
            else:
                print(f"WARN {name}: could not fetch '{crate}': HTTP {exc.code}")
            continue
        except OSError as exc:
            print(f"WARN {name}: could not fetch '{crate}': {exc}")
            continue

        checked += 1
        row = next((r for r in rows if r["vers"] == version), None)

        if row is None:
            latest = rows[-1]["vers"] if rows else "none"
            print(f"FAIL {name}: {crate}@{version} not on crates.io (latest {latest})")
            problems += 1
            continue

        if row.get("yanked"):
            print(f"FAIL {name}: {crate}@{version} is yanked")
            problems += 1
            continue

        declared = str(ing.get("datafusion", ""))
        actual = datafusion_major(row)

        # Ingredients pinned to the arrow train rather than DataFusion declare
        # an `arrow` major; that is the constraint worth verifying for them.
        declared_arrow = str(ing.get("arrow", "")) if ing.get("arrow") else None
        if declared_arrow:
            actual_arrow = arrow_major(row, crate)
            if actual_arrow is None:
                print(f"WARN {name}: {crate}@{version} declares arrow {declared_arrow}, "
                      f"but the crate has no arrow dependency")
            elif actual_arrow != declared_arrow:
                print(f"FAIL {name}: {crate}@{version} is built against arrow "
                      f"{actual_arrow}, but the ingredient declares {declared_arrow}")
                problems += 1
            else:
                print(f"ok   {name}: {crate}@{version} (arrow {actual_arrow}, "
                      f"pairs with datafusion {declared})")

            newest = rows[-1]["vers"]
            if newest != version:
                print(f"     note: {crate} has a newer release, {newest}")
            continue

        if declared == "any":
            note = f"requires datafusion {actual}" if actual else "no datafusion dependency"
            print(f"ok   {name}: {crate}@{version} ({note})")
        elif actual is None:
            print(f"WARN {name}: {crate}@{version} declares datafusion {declared}, "
                  f"but the crate has no datafusion dependency")
        elif actual != declared:
            print(f"FAIL {name}: {crate}@{version} requires datafusion {actual}, "
                  f"but the ingredient declares {declared}")
            problems += 1
        else:
            print(f"ok   {name}: {crate}@{version} (datafusion {actual})")

        newest = rows[-1]["vers"]
        if newest != version:
            print(f"     note: {crate} has a newer release, {newest}")

    for entry in skipped:
        print(f"skip {entry}")

    print(f"\n{checked} checked, {problems} problem(s), {len(skipped)} skipped")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
