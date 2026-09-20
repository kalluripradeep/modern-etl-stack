#!/usr/bin/env python3
"""Guards for the cross-pipeline benchmarks.

A benchmark that quietly reports a wrong number is worse than no benchmark:
nobody re-checks a figure that already looks plausible, and these figures are
going into a paper. The properties locked here are the ones whose failure
would be invisible in the output.

Runs offline. Nothing here connects to a pipeline.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
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


print("scripts/benchmark.py")
bench = load(ROOT / "scripts" / "benchmark.py", "benchmark")

# 1. Every query must render for every store with nothing left unsubstituted.
#    A stray "{orders}" reaching the database is an error the store reports,
#    but a stray placeholder that happens to be valid SQL is not.
for qname, template in bench.QUERIES.items():
    for store in bench.STORES:
        sql = bench.render(template, store)
        check(
            f"{qname} renders for {store}",
            "{" not in sql and "}" not in sql,
            f"unsubstituted placeholder in: {sql[:70]}",
        )

# 2. The three stores must render to *different* SQL. If a table-name map were
#    ever copied between stores, every pipeline would be measured against the
#    same one and the comparison would agree beautifully and mean nothing.
rendered = {s: bench.render(bench.QUERIES["count_orders"], s) for s in bench.STORES}
check(
    "each store queries its own tables",
    len(set(rendered.values())) == len(rendered),
    f"duplicate SQL across stores: {rendered}",
)

# 3. Percentiles must be nearest-rank, not interpolated. statistics.quantiles
#    would return a p95 no run produced, which for seven samples is a number
#    invented out of two real ones.
# Deliberately skewed and asymmetric: with evenly spaced samples the mean
# lands on a real observation, so an averaging bug would pass "is an observed
# sample" unnoticed. Here mean is 22.0, which is not in the list, and the
# nearest-rank p95 has to be the outlier itself.
samples = [1.0, 2.0, 3.0, 4.0, 100.0]
p95 = bench.percentile(samples, 95)
p50 = bench.percentile(samples, 50)
check("p95 is an observed sample", p95 in samples, f"got {p95}")
check("p50 is an observed sample", p50 in samples, f"got {p50}")
check("p95 keeps the outlier, does not average it away", p95 == 100.0, f"got {p95}")
check("p50 is the middle observation", p50 == 3.0, f"got {p50}")
check("p95 >= p50", p95 >= p50, f"p95={p95} p50={p50}")
check("percentile of empty is None", bench.percentile([], 95) is None)
check("single sample returns itself", bench.percentile([4.2], 95) == 4.2)

# 4. An unset TRINO_URL must raise rather than silently time a query that
#    never ran. The e2e suite treats "not deployed" as a skip; so must this.
bench.TRINO_URL = ""
try:
    bench.run_on("lakehouse", "SELECT 1")
    check("unset TRINO_URL is refused", False, "no error raised")
except bench.BenchmarkError:
    check("unset TRINO_URL is refused", True)
except Exception as e:  # noqa: BLE001
    check("unset TRINO_URL is refused", False, f"wrong error type: {type(e).__name__}")


print("\nairflow/dags — cross-pipe freshness")

# The DAG imports Airflow, which is not installed in every CI job, so read the
# freshness contract out of the source rather than importing the module.
dag_src = (ROOT / "airflow" / "dags" / "ingest_source_to_bronze.py").read_text(encoding="utf-8")

check(
    "freshness is probed for all three pipelines",
    all(f"'{p}':" in dag_src for p in ("warehouse", "mirror", "lakehouse")),
)
check(
    "the labelled metric is emitted",
    "etl.pipe_freshness." in dag_src,
)
check(
    "the pre-existing etl.freshness gauge is left alone",
    "etl.freshness." in dag_src,
    "renaming it would break the dashboards and alerts that reference it",
)

# The property that matters most: a pipeline that cannot be reached must
# produce no sample. Emitting 0.0 would read on a graph as perfect freshness
# for a pipeline that is down -- exactly backwards, and nobody would question
# a flat green line.
probe_block = dag_src[dag_src.index("def emit_pipe_freshness"):]
probe_block = probe_block[: probe_block.index("\ndef ")] if "\ndef " in probe_block else probe_block
check(
    "a failed probe is skipped, not recorded as zero",
    "log.warning" in probe_block and "_emit_metric" in probe_block,
)
check(
    "the emit is guarded by a None check",
    "is not None" in probe_block,
    "a null MAX() on an empty table would otherwise emit as 0.0",
)

# Both statsd mappings must carry the rule, or the metric reaches Prometheus
# as an unlabelled name and the three pipelines cannot be told apart.
for mapping in ("monitoring/statsd_mapping.yml", "k8s/airflow/helm-values.yaml"):
    text = (ROOT / mapping).read_text(encoding="utf-8")
    check(
        f"{mapping} maps pipe_freshness",
        "airflow.etl.pipe_freshness.*.*" in text and 'pipe: "$1"' in text,
    )

print()
if failures:
    print(f"{len(failures)} check(s) failed")
    sys.exit(1)
print("All benchmark guards passed")
