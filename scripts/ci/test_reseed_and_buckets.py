#!/usr/bin/env python3
"""Guards for the two failures a clean redeploy plus a reseed exposed.

Both were silent in their own way. The missing bucket surfaced as a
NoSuchBucket buried in a Spark stack trace, which reads like an Iceberg
problem. The UNIQUE constraint surfaced as a duplicate-key error naming the
source data, when the duplicate was a row the source no longer had.

Runs offline.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "airflow" / "dags"
failures = []


def check(label, condition, detail=""):
    if condition:
        print(f"  ok    {label}")
    else:
        print(f"  FAIL  {label}{(' — ' + detail) if detail else ''}")
        failures.append(label)


sys.path.insert(0, str(DAGS))
spec = importlib.util.spec_from_file_location("pipeline_config", DAGS / "pipeline_config.py")
pc = importlib.util.module_from_spec(spec)
sys.modules["pipeline_config"] = pc
spec.loader.exec_module(pc)

manifest = pc.load_manifest()

print("raw DDL — no UNIQUE outside the primary key")

for name, table in manifest["tables"].items():
    ddl = pc.raw_ddl(name, table)
    # The primary key stays: it is what the upsert conflicts on, and losing
    # it would turn every re-run into duplicate rows.
    check(f"{name}: primary key declared", "PRIMARY KEY" in ddl)
    check(
        f"{name}: no UNIQUE constraint",
        "UNIQUE" not in ddl.upper().replace("PRIMARY KEY", ""),
        "a reseed updates a retained row to a value another retained row "
        "holds, and ingest dies on every table",
    )

# The manifest still declares unique_columns, and it should: dbt reads that
# intent as a test. What must not happen is it becoming a hard constraint.
customers = manifest["tables"]["customers"]
check(
    "the manifest still records email as unique",
    "email" in customers.get("unique_columns", []),
    "dropping the declaration would lose the intent, not just the constraint",
)

drops = pc.drop_legacy_unique_ddl("customers", customers)
check("a migration is emitted for existing clusters", len(drops) == 1, str(drops))
check(
    "the migration is idempotent",
    all("IF EXISTS" in d for d in drops),
    "it runs on every ingest, so it has to be a no-op after the first time",
)
check(
    "the migration names the constraint Postgres generates",
    all("customers_source_email_key" in d for d in drops),
    str(drops),
)
check(
    "tables without unique_columns emit nothing",
    pc.drop_legacy_unique_ddl("orders", manifest["tables"]["orders"]) == [],
)

# dbt must still assert what the constraint used to. Dropping the constraint
# is only safe because the condition is still detected somewhere.
sources = (ROOT / "dbt" / "models" / "sources.yml").read_text(encoding="utf-8")
email_block = sources[sources.index("- name: email"):] if "- name: email" in sources else ""
check(
    "dbt still tests email uniqueness",
    "unique" in email_block[:200],
    "without this the constraint's coverage is simply gone",
)

print("\nsilver bucket")

silver_dag = (DAGS / "spark_transform_silver.py").read_text(encoding="utf-8")
check(
    "the silver DAG ensures its buckets",
    "ensure_buckets" in silver_dag and "create_bucket" in silver_dag,
)
check(
    "it creates the Iceberg warehouse bucket",
    "'silver'" in silver_dag or '"silver"' in silver_dag,
)
check(
    "it runs before the transforms",
    "ensure_buckets >> transform_orders" in silver_dag,
    "defining the task is not enough -- unwired, or wired after the job "
    "that needs the bucket, it helps nobody",
)
# The catalog's warehouse and the bucket created have to be the same place.
check(
    "the bucket matches the catalog warehouse",
    "s3a://silver/" in silver_dag,
    "if the warehouse URI moves, this guard should fail rather than let the "
    "DAG create a bucket nothing writes to",
)

print()
if failures:
    print(f"{len(failures)} check(s) failed")
    sys.exit(1)
print("All reseed/bucket guards passed")
