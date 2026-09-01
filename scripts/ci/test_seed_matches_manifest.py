#!/usr/bin/env python3
"""The seeded source schema must satisfy the pipeline manifest.

airflow/dags/config/pipelines.yml is the single source of truth for which
columns flow through the platform, and the ingestion DAG selects them by name.
If the seeder creates a table without one, ingestion fails with

    Source table 'products' is missing name, description, stock_quantity

and the message blames the source data, which is where the diagnosis stops.

That happened for real (#144). scripts/test_transactions.py carried its own
CREATE TABLE statements -- customers with name/city/country, products with
product_name, order_items with price instead of unit_price -- and dropped the
real tables before creating them. Every run of the e2e suite left the source
incompatible with the manifest, so the next ingestion failed, and re-seeding
fixed it only until the tests ran again. Two definitions of one schema, drifting.

The test now reuses the seeder. This keeps the seeder itself honest, and reads
both files rather than trusting either.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SEEDER = ROOT / "sample-data" / "generate_ecommerce.py"
MANIFEST = ROOT / "airflow" / "dags" / "config" / "pipelines.yml"


def seeded_columns():
    """Column names per table, read out of the seeder's CREATE TABLE statements."""
    src = SEEDER.read_text(encoding="utf-8")
    tables = {}
    for match in re.finditer(r"CREATE TABLE (\w+) \((.*?)\n\s*\)", src, re.S):
        name, body = match.group(1), match.group(2)
        cols = []
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            first = line.split()[0].strip(",")
            if first.upper() in {"PRIMARY", "FOREIGN", "UNIQUE", "CONSTRAINT", "CHECK"}:
                continue
            cols.append(first)
        tables[name] = cols
    return tables


def main():
    import yaml

    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    seeded = seeded_columns()
    assert seeded, "no CREATE TABLE statements found in the seeder"

    problems = []
    for table, spec in manifest["tables"].items():
        if table not in seeded:
            problems.append(f"{table}: the seeder never creates it")
            continue
        wanted = list(spec["columns"])
        cursor = spec.get("cursor_column")
        if cursor and cursor not in wanted:
            wanted.append(cursor)
        missing = [c for c in wanted if c not in seeded[table]]
        if missing:
            problems.append(
                f"{table}: seeder is missing {', '.join(missing)} "
                f"(it creates {', '.join(seeded[table])})"
            )

    if problems:
        print("The seeded schema does not satisfy the manifest:")
        for p in problems:
            print(f"  - {p}")
        print("\nIngestion selects the manifest's columns by name, so it would "
              "fail on every one of these.")
        sys.exit(1)

    for table in manifest["tables"]:
        print(f"  ok    {table}: {len(manifest['tables'][table]['columns'])} manifest columns present")
    print("seeded schema matches the manifest: OK")


if __name__ == "__main__":
    main()
