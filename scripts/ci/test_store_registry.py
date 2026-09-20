#!/usr/bin/env python3
"""Guards for the store registry.

The registry exists because "the three pipelines land the same tables under
different names" was written down in four places, and a convention repeated
four times drifts at whichever copy is read least. These checks are about
keeping it at one.

The generated TypeScript is not checked for freshness here -- the existing
pipeline-config-sync job already fails when any generated artifact differs
from the manifest, and stores.generated.ts is one of its targets now.

Runs offline.
"""

import importlib.util
import os
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "airflow" / "dags"
MANIFEST = DAGS / "config" / "pipelines.yml"
failures = []


def check(label, condition, detail=""):
    if condition:
        print(f"  ok    {label}")
    else:
        print(f"  FAIL  {label}{(' — ' + detail) if detail else ''}")
        failures.append(label)


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


pc = load(DAGS / "pipeline_config.py", "pipeline_config")
manifest = pc.load_manifest(str(MANIFEST))

print("manifest — the registry is declared and validated")

check("stores are declared", bool(manifest.get("stores")))
check(
    "every pipeline is represented",
    set(manifest["stores"]) >= {"warehouse", "lakehouse", "mirror"},
    str(sorted(manifest["stores"])),
)


def rejects(label, mutate):
    """A bad declaration must fail at load, not as bad SQL hours later."""
    m = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    mutate(m)
    fd, path = tempfile.mkstemp(suffix=".yml")
    os.close(fd)
    Path(path).write_text(yaml.safe_dump(m), encoding="utf-8")
    try:
        pc.load_manifest(path)
        check(label, False, "loaded without complaint")
    except ValueError:
        check(label, True)
    finally:
        os.unlink(path)


# The first of these is the dangerous one. A table template with no {table}
# resolves every table to a single name -- not an error any database reports,
# just a query against the wrong data, and the benchmark would then show three
# pipelines agreeing closely for a reason that has nothing to do with them.
rejects("rejects a table template with no {table}",
        lambda m: m["stores"]["mirror"].update(table="mirror.orders_current"))
rejects("rejects two stores sharing a table template",
        lambda m: m["stores"]["lakehouse"].update(table=m["stores"]["mirror"]["table"]))
rejects("rejects date_trunc with no placeholder",
        lambda m: m["stores"]["warehouse"].update(date_trunc="date(order_date)"))
rejects("rejects freshness missing {col}",
        lambda m: m["stores"]["warehouse"].update(freshness="SELECT 1 FROM {table}"))
rejects("rejects a store missing a required key",
        lambda m: m["stores"]["warehouse"].pop("freshness"))
rejects("rejects a manifest with no stores",
        lambda m: m.pop("stores"))

print("\nconsumers read the registry rather than restating it")

bench = load(ROOT / "scripts" / "benchmark.py", "benchmark")
check(
    "benchmark builds its stores from the manifest",
    set(bench.STORES) == set(manifest["stores"]),
    f"{sorted(bench.STORES)} vs {sorted(manifest['stores'])}",
)
for name, store in manifest["stores"].items():
    expected = store["table"].format(table="orders")
    check(
        f"benchmark resolves {name}.orders to {expected}",
        bench.STORES[name]["tables"]["orders"] == expected,
        bench.STORES[name]["tables"].get("orders"),
    )

# Physical names must not be spelled out anywhere but the manifest. A literal
# here is a copy that will outlive the next rename.
sources = {
    "scripts/benchmark.py": (ROOT / "scripts" / "benchmark.py").read_text(encoding="utf-8"),
    "airflow/dags/ingest_source_to_bronze.py":
        (DAGS / "ingest_source_to_bronze.py").read_text(encoding="utf-8"),
    "ui/src/app/api/chat/route.ts":
        (ROOT / "ui" / "src" / "app" / "api" / "chat" / "route.ts").read_text(encoding="utf-8"),
}
for path, text in sources.items():
    for literal in ("mirror.orders_current", "iceberg.lake.orders"):
        check(
            f"{path} does not hardcode {literal}",
            literal not in text,
            "resolve it through the registry instead",
        )

dag_src = sources["airflow/dags/ingest_source_to_bronze.py"]
check(
    "the freshness probe reads dialects from the manifest",
    "MANIFEST['stores']" in dag_src,
    "it was the fourth copy of the naming convention",
)

print("\nadding a store stays a config change")

# The claim this registry makes is that a fourth engine is configuration, not
# a patch. Assert it by adding one to an in-memory manifest and confirming it
# resolves -- if wiring a store ever needs code again, this fails.
m = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
m["stores"]["duckdb"] = {
    "label": "Pipe 2b · DuckDB over the same Iceberg tables",
    "dialect": "duckdb",
    "table": "lake_{table}",
    "date_trunc": "cast({} as date)",
    "freshness": "SELECT epoch(now()) - epoch(max({col})) FROM {table}",
}
fd, path = tempfile.mkstemp(suffix=".yml")
os.close(fd)
Path(path).write_text(yaml.safe_dump(m), encoding="utf-8")
try:
    extended = pc.load_manifest(path)
    check("a fourth store validates with no code change", "duckdb" in extended["stores"])
    check(
        "and resolves its table names",
        pc.store_table(extended, "duckdb", "orders") == "lake_orders",
        pc.store_table(extended, "duckdb", "orders"),
    )
finally:
    os.unlink(path)

print()
if failures:
    print(f"{len(failures)} check(s) failed")
    sys.exit(1)
print("All store registry guards passed")
