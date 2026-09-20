#!/usr/bin/env python3
"""Compare the three pipelines against each other on identical data.

The repository already proves the three pipelines are *correct*
(scripts/test_transactions.py). It has never measured what each one costs.
That is the gap this fills, and it is the only question the architecture is
really placed to answer: one operational source feeds a batch warehouse, an
Iceberg lakehouse and a CDC mirror at the same time, on the same rows, so the
three can be compared without the usual caveat that somebody benchmarked
their own product on their own dataset.

Three measurements, none of which need new infrastructure:

  B2  query latency   the same logical question asked of all three stores,
                      p50/p95 over several repetitions
  B3  storage         bytes on disk per pipeline, and bytes per source row
  B4  scale           the above repeated at 10k / 1M / 10M orders

End-to-end freshness (B1) is deliberately not here. Freshness has to be
sampled continuously while the pipelines run, not once from a laptop, so it
is emitted as a gauge by the ingestion DAG and read from Prometheus.

Usage:

    # against a running stack (scripts/test_e2e.sh exports the same vars)
    python scripts/benchmark.py

    # the scale ladder -- reseeds the source, so never point it at anything
    # whose contents matter
    python scripts/benchmark.py --scale 10000
    python scripts/benchmark.py --scale 1000000

    python scripts/benchmark.py --json results.json   # machine-readable too

Every store is queried read-only. The script never writes to a pipeline,
only to postgres-source and only when --scale is given.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2
import requests

# ── Connection settings ──────────────────────────────────────────────────────
# These are the same names scripts/test_transactions.py reads, with the same
# defaults, so scripts/test_e2e.sh can export once and run both.
SOURCE = dict(
    host=os.environ.get("SOURCE_DB_HOST", "localhost"),
    port=int(os.environ.get("SOURCE_DB_PORT", 5432)),
    dbname=os.environ.get("SOURCE_DB_NAME", "sourcedb"),
    user=os.environ.get("SOURCE_DB_USER", "sourceuser"),
    password=os.environ.get("SOURCE_DB_PASSWORD", "sourcepass"),
)
DEST = dict(
    host=os.environ.get("DEST_DB_HOST", "localhost"),
    port=int(os.environ.get("DEST_DB_PORT", 5432)),
    dbname=os.environ.get("DEST_DB_NAME", "destdb"),
    user=os.environ.get("DEST_DB_USER", "destuser"),
    password=os.environ.get("DEST_DB_PASSWORD", "destpass"),
)
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "chuser")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "chpass")
# Unset means "not deployed here", which is a skipped pipeline rather than a
# failure -- the same convention the e2e suite uses.
TRINO_URL = os.environ.get("TRINO_URL", "")

REPEATS = int(os.environ.get("BENCHMARK_REPEATS", "7"))
WARMUP = int(os.environ.get("BENCHMARK_WARMUP", "1"))


# ── The three stores ─────────────────────────────────────────────────────────
# Each pipeline lands the same four tables under different names, and each
# speaks a different SQL dialect. Rather than write every query three times,
# a query is written once against placeholders and rendered per store. That
# keeps the comparison honest: a difference in the numbers cannot come from
# somebody having hand-tuned one of the three.
STORES = {
    "warehouse": {
        "label": "Pipe 1 · warehouse (PostgreSQL)",
        "tables": {
            "orders": "raw.orders_source",
            "order_items": "raw.order_items_source",
            "customers": "raw.customers_source",
            "products": "raw.products_source",
        },
        "date_of": "date({})",
    },
    "lakehouse": {
        "label": "Pipe 2 · lakehouse (Iceberg via Trino)",
        "tables": {
            "orders": "iceberg.lake.orders",
            "order_items": "iceberg.lake.order_items",
            "customers": "iceberg.lake.customers",
            "products": "iceberg.lake.products",
        },
        "date_of": "cast({} as date)",
    },
    "mirror": {
        "label": "Pipe 3 · mirror (ClickHouse)",
        "tables": {
            "orders": "mirror.orders_current",
            "order_items": "mirror.order_items_current",
            "customers": "mirror.customers_current",
            "products": "mirror.products_current",
        },
        "date_of": "toDate({})",
    },
}

# ── B2: the query suite ──────────────────────────────────────────────────────
# Five questions, chosen to exercise different shapes rather than to flatter
# any one engine: a trivial count, a grouped aggregate, a two-table join, a
# three-table join, and an unindexed full scan.
#
# These run against the landed source tables in each store rather than against
# the warehouse's modelled gold.* star schema. That is on purpose -- gold.*
# exists in Pipe 1 only, so including it would measure dbt modelling rather
# than the storage and engine, and the comparison would not mean anything.
# Comparing the modelled layers is a separate exercise.
QUERIES = {
    "count_orders": """
        SELECT count(*) FROM {orders}
    """,
    "daily_revenue": """
        SELECT {date_of_order_date} AS d, sum(total_amount) AS revenue
        FROM {orders}
        WHERE status <> 'cancelled'
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 30
    """,
    "top_products": """
        SELECT p.name, sum(oi.quantity * oi.unit_price) AS revenue
        FROM {order_items} oi
        JOIN {products} p ON p.product_id = oi.product_id
        GROUP BY p.name
        ORDER BY revenue DESC
        LIMIT 10
    """,
    "revenue_by_state": """
        SELECT c.state, count(DISTINCT o.customer_id) AS customers,
               sum(oi.quantity * oi.unit_price) AS revenue
        FROM {order_items} oi
        JOIN {orders} o ON o.order_id = oi.order_id
        JOIN {customers} c ON c.customer_id = o.customer_id
        GROUP BY c.state
        ORDER BY revenue DESC
        LIMIT 20
    """,
    "full_scan": """
        SELECT avg(total_amount), min(total_amount), max(total_amount)
        FROM {orders}
    """,
}


def render(sql: str, store: str) -> str:
    """Render a placeholder query for one store's names and dialect."""
    cfg = STORES[store]
    out = sql
    for logical, physical in cfg["tables"].items():
        out = out.replace("{" + logical + "}", physical)
    # Date truncation is the one expression that genuinely differs between
    # the three dialects, so it gets a placeholder of its own rather than
    # three copies of every query that needs it.
    out = out.replace("{date_of_order_date}", cfg["date_of"].format("order_date"))
    return " ".join(out.split())


# ── Store clients ────────────────────────────────────────────────────────────
# ponytail: ch_query/trino_query are near-copies of the helpers in
# scripts/test_transactions.py. Extracting them into a shared module would
# mean changing that file's import graph, which its CI guard imports by path;
# not worth destabilising a passing test for thirty lines. Merge them if a
# third caller ever appears.
class BenchmarkError(RuntimeError):
    """A store could not be reached or rejected the query."""


def pg_query(conn_kwargs: dict, sql: str):
    with psycopg2.connect(**conn_kwargs) as conn:
        # Read-only is enforced on the session, not just promised in a
        # comment: this script points at live pipelines.
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


def ch_query(sql: str):
    r = requests.post(
        f"{CLICKHOUSE_URL}/?default_format=JSONCompact&readonly=1",
        headers={"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD},
        data=sql.encode("utf-8"),
        timeout=120,
    )
    if r.status_code in (401, 403):
        raise BenchmarkError(
            f"ClickHouse rejected user '{CLICKHOUSE_USER}' (HTTP {r.status_code}). "
            "Set CLICKHOUSE_USER/CLICKHOUSE_PASSWORD to match the deployment."
        )
    r.raise_for_status()
    return r.json().get("data", [])


def trino_query(sql: str):
    """Run one statement over Trino's REST protocol, following nextUri.

    Results arrive across pages and the first page usually carries no rows,
    so a single POST reads an empty result and times a query that never ran.
    """
    if not TRINO_URL:
        raise BenchmarkError("TRINO_URL unset — Pipe 2 not reachable from here")
    hdr = {"X-Trino-User": "benchmark"}
    r = requests.post(f"{TRINO_URL}/v1/statement", data=sql.encode("utf-8"), headers=hdr, timeout=120)
    rows = []
    while True:
        r.raise_for_status()
        page = r.json()
        if page.get("error"):
            raise BenchmarkError(page["error"].get("message", "unknown Trino error"))
        rows.extend(page.get("data") or [])
        nxt = page.get("nextUri")
        if not nxt:
            return rows
        r = requests.get(nxt, headers=hdr, timeout=120)


def run_on(store: str, sql: str):
    if store == "warehouse":
        return pg_query(DEST, sql)
    if store == "mirror":
        return ch_query(sql)
    if store == "lakehouse":
        return trino_query(sql)
    raise ValueError(store)


# ── Output helpers ───────────────────────────────────────────────────────────
def step(title: str):
    print(f"\n{'=' * 64}\n  {title}\n{'=' * 64}")


def table(headers, rows):
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) if rows else len(str(h))
              for i, h in enumerate(headers)]
    line = "  " + "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    print(line)
    print("  " + "  ".join("-" * w for w in widths))
    for r in rows:
        print("  " + "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(r)))


def percentile(values, pct):
    """Nearest-rank percentile.

    statistics.quantiles interpolates, which invents a p95 that no run
    actually produced. With seven samples that is misleading, so take the
    real observation at the rank instead.
    """
    if not values:
        return None
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, int(round(pct / 100.0 * len(ordered) + 0.5)) - 1))
    return ordered[k]


# ── B2: query latency ────────────────────────────────────────────────────────
def bench_queries():
    step("B2 — Query latency: the same question asked of all three pipelines")
    results = {}
    rows = []
    for qname, template in QUERIES.items():
        for store in STORES:
            sql = render(template, store)
            timings = []
            error = None
            try:
                for _ in range(WARMUP):
                    run_on(store, sql)
                for _ in range(REPEATS):
                    t0 = time.perf_counter()
                    run_on(store, sql)
                    timings.append((time.perf_counter() - t0) * 1000.0)
            except Exception as e:  # noqa: BLE001 - one dead store must not end the run
                error = str(e).split("\n")[0][:80]
            if error:
                rows.append([qname, store, "—", "—", f"failed: {error}"])
                results.setdefault(qname, {})[store] = {"error": error}
            else:
                p50 = percentile(timings, 50)
                p95 = percentile(timings, 95)
                rows.append([qname, store, f"{p50:.1f}", f"{p95:.1f}", f"n={len(timings)}"])
                results.setdefault(qname, {})[store] = {
                    "p50_ms": round(p50, 2), "p95_ms": round(p95, 2), "samples": timings,
                }
    table(["query", "pipeline", "p50 ms", "p95 ms", "note"], rows)
    print(f"\n  {WARMUP} warmup run(s) discarded, {REPEATS} timed runs per cell.")
    print("  Percentiles are nearest-rank, not interpolated.")
    return results


# ── B3: storage ──────────────────────────────────────────────────────────────
def bench_storage(source_rows):
    step("B3 — Storage: bytes on disk per pipeline")
    results = {}

    def safe(fn, label):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            print(f"  {label} storage unavailable: {str(e).splitlines()[0][:70]}")
            return None

    def warehouse_bytes():
        total = 0
        for physical in STORES["warehouse"]["tables"].values():
            rec = pg_query(DEST, f"SELECT pg_total_relation_size('{physical}')")
            total += int(rec[0][0] or 0)
        return total

    def mirror_bytes():
        rec = ch_query(
            "SELECT sum(bytes_on_disk) FROM system.parts "
            "WHERE database = 'mirror' AND active"
        )
        return int(float(rec[0][0])) if rec and rec[0] and rec[0][0] is not None else 0

    def lakehouse_bytes():
        # Iceberg keeps data files and metadata separately; $files covers the
        # data a query actually reads. Snapshot and manifest overhead is
        # reported next to it rather than folded in, because the two grow for
        # different reasons and maintenance only reclaims one of them.
        total = 0
        for logical in STORES["lakehouse"]["tables"]:
            # nosec B608 - `logical` is a key of the STORES literal above,
            # one of four names fixed in this file. Iceberg metadata tables
            # are addressed by identifier ("orders$files"), and an identifier
            # cannot be bound as a parameter in any dialect, so there is
            # nothing to parameterise even in principle.
            rec = trino_query(
                f'SELECT sum(file_size_in_bytes) FROM iceberg.lake."{logical}$files"'  # nosec B608
            )
            if rec and rec[0] and rec[0][0] is not None:
                total += int(rec[0][0])
        return total

    def lakehouse_snapshots():
        total = 0
        for logical in STORES["lakehouse"]["tables"]:
            # nosec B608 - same as above: a fixed key, not input.
            rec = trino_query(
                f'SELECT count(*) FROM iceberg.lake."{logical}$snapshots"'  # nosec B608
            )
            if rec and rec[0] and rec[0][0] is not None:
                total += int(rec[0][0])
        return total

    sizes = {
        "warehouse": safe(warehouse_bytes, "warehouse"),
        "lakehouse": safe(lakehouse_bytes, "lakehouse"),
        "mirror": safe(mirror_bytes, "mirror"),
    }
    snaps = safe(lakehouse_snapshots, "lakehouse snapshot")

    # Normalise by the rows each store actually holds, not by the source's
    # count. The three pipelines are asynchronous and routinely hold different
    # amounts -- the lakehouse merges and keeps deleted rows, the mirror drops
    # them, and a reseeded source can be an order of magnitude smaller than
    # what the warehouse still has. Dividing every store's bytes by the source
    # count produced a figure that looked entirely reasonable (11 kB/row for
    # Postgres) and was inflated fiftyfold.
    counts = {}
    for store in sizes:
        try:
            t = STORES[store]["tables"]
            sql = (f'SELECT (SELECT count(*) FROM {t["orders"]}) + '  # nosec B608
                   f'(SELECT count(*) FROM {t["order_items"]})')
            rec = run_on(store, sql)
            counts[store] = int(float(rec[0][0])) if rec and rec[0] else 0
        except Exception:  # noqa: BLE001
            counts[store] = 0

    rows = []
    for store, nbytes in sizes.items():
        if nbytes is None:
            rows.append([STORES[store]["label"], "-", "-", "-"])
            continue
        n = counts.get(store, 0)
        per_row = (nbytes / n) if n else 0
        rows.append([STORES[store]["label"], f"{nbytes / 1024 / 1024:.1f} MiB",
                     f"{n:,}", f"{per_row:.0f} B" if n else "no rows"])
        results[store] = {"bytes": nbytes, "rows": n, "bytes_per_row": round(per_row, 2)}
    table(["pipeline", "on disk", "rows held", "per row"], rows)

    print()
    print(f"  Source holds {source_rows:,} rows (orders + order_items).")
    spread = [n for n in counts.values() if n]
    if spread and max(spread) > 2 * min(spread):
        print("  The pipelines hold materially different row counts, so they are")
        print("  not all caught up. Per-row figures remain valid -- each is")
        print("  divided by its own rows -- but the totals are not comparable")
        print("  until the DAGs have run against the same source.")
    if snaps is not None:
        print(f"  Iceberg snapshots retained across the four tables: {snaps}")
        print("  Re-run after the maintenance DAG to see what expire_snapshots reclaims.")
        results["lakehouse_snapshots"] = snaps
    print("  Data files only for Iceberg; warehouse figure is pg_total_relation_size")
    print("  (heap + indexes + TOAST), so the three are not like-for-like internals.")
    return results


def count_source_rows():
    try:
        rec = pg_query(
            SOURCE,
            "SELECT (SELECT count(*) FROM orders) + (SELECT count(*) FROM order_items)",
        )
        return int(rec[0][0])
    except Exception as e:  # noqa: BLE001
        print(f"  Could not count source rows: {str(e).splitlines()[0][:70]}")
        return 0


# ── B4: scale ────────────────────────────────────────────────────────────────
def reseed(n_orders):
    """Reseed postgres-source with n_orders before measuring.

    Delegates to the real seeder rather than reimplementing inserts -- that
    divergence is exactly what broke the e2e suite before (#170), where the
    test's own CREATE TABLE statements did not match the manifest and left
    the source incompatible on every run.
    """
    step(f"B4 — Reseeding postgres-source with {n_orders:,} orders")
    seeder = Path(__file__).resolve().parents[1] / "sample-data" / "generate_ecommerce.py"
    if not seeder.exists():
        raise BenchmarkError(f"seeder not found at {seeder}")
    import importlib.util

    spec = importlib.util.spec_from_file_location("generate_ecommerce", seeder)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["generate_ecommerce"] = mod

    # Scale the dimensions with the facts. Leaving customers at 100 while
    # orders go to ten million makes every join degenerate -- a hundred
    # groups no matter how much data there is -- which would flatter whichever
    # engine caches grouped results best and measure nothing real.
    os.environ["NUM_ORDERS"] = str(n_orders)
    os.environ.setdefault("NUM_CUSTOMERS", str(max(100, n_orders // 100)))
    os.environ.setdefault("NUM_PRODUCTS", str(max(50, n_orders // 1000)))

    t0 = time.perf_counter()
    spec.loader.exec_module(mod)
    # main() sits behind an `if __name__ == "__main__"` guard, so importing
    # the module defines the generators without running any of them.
    mod.main()
    print(f"  seeded in {time.perf_counter() - t0:.1f}s")
    print("  Now run the pipelines before measuring, or the numbers describe")
    print("  the previous dataset:")
    print("    airflow dags trigger ingest_source_to_bronze")
    print("    airflow dags trigger spark_transform_silver")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--scale", type=int, metavar="N",
                    help="reseed postgres-source with N orders first (destructive)")
    ap.add_argument("--json", metavar="PATH", help="also write results as JSON")
    ap.add_argument("--skip-storage", action="store_true", help="run B2 only")
    args = ap.parse_args()

    if args.scale:
        reseed(args.scale)
        print("\n  Stopping here: the pipelines have not run against the new data yet.")
        print("  Re-run without --scale once they have.")
        return 0

    print()
    print(f"  warehouse  {DEST['host']}:{DEST['port']}/{DEST['dbname']}")
    print(f"  lakehouse  {TRINO_URL}")
    print(f"  mirror     {CLICKHOUSE_URL}")

    out = {"queries": bench_queries()}
    if not args.skip_storage:
        out["storage"] = bench_storage(count_source_rows())

    failed = sum(1 for q in out["queries"].values() for c in q.values() if "error" in c)
    step("Summary")
    total = len(QUERIES) * len(STORES)
    print(f"  {total - failed} of {total} query/pipeline cells measured, {failed} failed.")
    if failed:
        print("  Failed cells are shown above; a store that is down is not a zero.")

    if args.json:
        Path(args.json).write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"  Wrote {args.json}")
    return 1 if failed == total else 0


if __name__ == "__main__":
    sys.exit(main())
