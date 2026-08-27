#!/usr/bin/env python3
"""
End-to-end transactional test for the ETL pipeline.

What this tests:
  Step 1  Seed 50 customers, 20 products, 200 orders into postgres-source
  Step 2  Register the Debezium CDC connector (if not already registered)
  Step 3  Run the extract → validate → load pipeline (bronze parquet to
          MinIO, staged COPY upsert into raw.orders_source — the same
          targets the real ingestion DAG writes)
  Step 4  Simulate live transactions: UPDATE status, CANCEL orders, DELETE rows
  Step 5  Verify the ClickHouse mirror (Pipe 3's real consumer) reflects
          every change: counts, deletes gone, cancellations applied
  Step 6  Verify the Iceberg lakehouse (Pipe 2) is readable through Trino
  Step 7  Verify the dbt marts (Pipe 1) exist and are populated
  Step 8  Print pass / fail / skip report

Coverage, stated honestly (#149): Step 3 reproduces the ingestion DAG's
writes rather than triggering the DAG, so Pipes 1 and 2 are checked by their
outputs -- the dbt marts and the Iceberg tables can only be there if the real
DAG and its dbt task group ran. Steps 6 and 7 report SKIP, never PASS, when
their endpoint is not configured, because a check that cannot run is not a
check that passed. The final line names what was verified instead of claiming
the whole pipeline is healthy.

Usage (local docker-compose):
  DEST_DB_PORT=5433 MINIO_ROOT_PASSWORD=minioadmin python scripts/test_transactions.py

Usage (Kubernetes — port-forward first):
  kubectl port-forward svc/postgres-source 5433:5432 -n etl &
  kubectl port-forward svc/postgres-dest   5434:5432 -n etl &
  kubectl port-forward svc/kafka-connect   8083:8083 -n etl &
  kubectl port-forward svc/minio           9000:9000 -n etl &
  kubectl port-forward svc/clickhouse      8123:8123 -n etl &
  SOURCE_DB_PORT=5433 DEST_DB_PORT=5434 python scripts/test_transactions.py
"""

import json
import os
import random
import shutil
import subprocess  # nosec B404 - fixed argv, no shell
import sys
import time
from datetime import datetime, timedelta

import psycopg2
import requests
from faker import Faker

# ── Connection config ──────────────────────────────────────────────────────────
SOURCE = dict(
    host=os.environ.get("SOURCE_DB_HOST", "localhost"),
    port=int(os.environ.get("SOURCE_DB_PORT", 5432)),
    database=os.environ.get("SOURCE_DB_NAME", "sourcedb"),
    user=os.environ.get("SOURCE_DB_USER", "sourceuser"),
    password=os.environ.get("SOURCE_DB_PASSWORD", "sourcepass"),
)
DEST = dict(
    host=os.environ.get("DEST_DB_HOST", "localhost"),
    port=int(os.environ.get("DEST_DB_PORT", 5432)),
    database=os.environ.get("DEST_DB_NAME", "destdb"),
    user=os.environ.get("DEST_DB_USER", "destuser"),
    password=os.environ.get("DEST_DB_PASSWORD", "destpass"),
)
CONNECT_URL     = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER      = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD  = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
CLICKHOUSE_URL  = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "chuser")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "chpass")
# Unset means "not deployed here", which is a skip rather than a failure.
TRINO_URL       = os.environ.get("TRINO_URL", "")

STATUSES  = ["pending", "processing", "shipped", "delivered", "cancelled"]
fake = Faker()

results = []   # accumulated pass/fail records


def step(name):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")


def ok(msg, detail=""):
    tag = f"  ✓  {msg}"
    if detail:
        tag += f"  ({detail})"
    print(tag)
    results.append(("PASS", msg))


def fail(msg, detail=""):
    tag = f"  ✗  {msg}"
    if detail:
        tag += f"  ({detail})"
    print(tag)
    results.append(("FAIL", msg))


def skip(msg, detail=""):
    """Record a check that could not run.

    Deliberately not a pass. Counting an unrunnable check as green is how this
    suite came to report a healthy pipeline while never reading the lakehouse
    at all (#149).
    """
    tag = f"  ~  {msg}"
    if detail:
        tag += f"  ({detail})"
    print(tag)
    results.append(("SKIP", msg))


# ── Step 1: Seed source database ──────────────────────────────────────────────
def seed_source():
    step("STEP 1 — Seed postgres-source with transactional test data")

    conn = psycopg2.connect(**SOURCE)

    with conn.cursor() as cur:
        # Fresh test schema — drop only test tables
        cur.execute("DROP TABLE IF EXISTS order_items CASCADE")
        cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS customers CASCADE")
        cur.execute("DROP TABLE IF EXISTS products CASCADE")

        cur.execute("""
            CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                name        VARCHAR(100),
                email       VARCHAR(100) UNIQUE,
                city        VARCHAR(100),
                country     VARCHAR(100),
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE products (
                product_id   SERIAL PRIMARY KEY,
                product_name VARCHAR(200),
                category     VARCHAR(50),
                price        DECIMAL(10,2),
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                order_id     SERIAL PRIMARY KEY,
                customer_id  INT REFERENCES customers(customer_id),
                order_date   TIMESTAMP,
                total_amount DECIMAL(10,2),
                status       VARCHAR(20),
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE order_items (
                item_id    SERIAL PRIMARY KEY,
                order_id   INT REFERENCES orders(order_id),
                product_id INT REFERENCES products(product_id),
                quantity   INT,
                price      DECIMAL(10,2)
            )
        """)
        conn.commit()

        # 50 customers
        for _ in range(50):
            try:
                cur.execute(
                    "INSERT INTO customers (name, email, city, country) VALUES (%s, %s, %s, %s)",
                    (fake.name()[:100], fake.email()[:100], fake.city()[:100], fake.country()[:100]),
                )
            except Exception:
                conn.rollback()
        conn.commit()

        # 20 products
        for _ in range(20):
            cur.execute(
                "INSERT INTO products (product_name, category, price) VALUES (%s, %s, %s)",
                (fake.catch_phrase()[:200], random.choice(["Electronics","Clothing","Books","Home","Sports"]),
                 round(random.uniform(9.99, 499.99), 2)),
            )
        conn.commit()

        # 200 orders with 1-3 items each
        cur.execute("SELECT customer_id FROM customers")
        cust_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT product_id, price FROM products")
        prod_price = {r[0]: float(r[1]) for r in cur.fetchall()}
        prod_ids = list(prod_price.keys())

        start = datetime.now() - timedelta(days=30)
        for i in range(200):
            order_date = start + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
            cur.execute(
                "INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES (%s,%s,%s,%s) RETURNING order_id",
                (random.choice(cust_ids), order_date, 0, random.choice(STATUSES)),
            )
            oid = cur.fetchone()[0]
            total = 0.0
            for _ in range(random.randint(1, 3)):
                pid = random.choice(prod_ids)
                qty = random.randint(1, 4)
                total += prod_price[pid] * qty
                cur.execute(
                    "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s,%s,%s,%s)",
                    (oid, pid, qty, prod_price[pid]),
                )
            cur.execute("UPDATE orders SET total_amount=%s WHERE order_id=%s", (round(total, 2), oid))
            if (i + 1) % 50 == 0:
                conn.commit()
                print(f"  seeded {i+1}/200 orders…")
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM orders")
        n = cur.fetchone()[0]
    conn.close()

    if n == 200:
        ok("Seeded 200 orders into postgres-source")
    else:
        fail(f"Expected 200 orders, got {n}")

    return n


# ── Step 2: Register Debezium connector ───────────────────────────────────────
def register_connector():
    step("STEP 2 — Register Debezium CDC connector")

    # Check if already registered
    try:
        r = requests.get(f"{CONNECT_URL}/connectors/orders-cdc-connector", timeout=5)
        if r.status_code == 200:
            ok("Connector already registered — skipping")
            return
    except Exception as e:
        fail("Cannot reach Kafka Connect", str(e))
        print("  → Make sure kafka-connect is running and port-forwarded if using K8s")
        return

    payload = {
        "name": "orders-cdc-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": SOURCE["host"],
            "database.port": str(SOURCE["port"]),
            "database.user": SOURCE["user"],
            "database.password": SOURCE["password"],
            "database.dbname": SOURCE["database"],
            "topic.prefix": "cdc",
            "table.include.list": "public.orders",
            "plugin.name": "pgoutput",
            "slot.name": "debezium_orders_slot",
            "publication.name": "debezium_orders_pub",
            "snapshot.mode": "initial",
        },
    }
    r = requests.post(f"{CONNECT_URL}/connectors", json=payload, timeout=10)
    if r.status_code in (200, 201):
        ok("Debezium connector registered")
    else:
        fail("Failed to register connector", r.text[:120])


# ── Step 3: Run extract pipeline ──────────────────────────────────────────────
def run_extract_pipeline():
    """Run the real ingestion DAG over the rows step 1 seeded.

    This used to re-implement the extract: read the source with pandas, write
    parquet, upload it, COPY into raw. Every part of that was a second
    implementation of something the platform already does, and each one caused
    a bug.

    It wrote to bronze/orders/ while the DAG writes <table>_source/, so Pipe 2
    never saw any of it (#152). Pointing it at orders_source/ fixed that and
    created two worse problems: the files were named part-00000.parquet with no
    run stamp, so every run overwrote the last -- the exact collision the DAG's
    run stamp exists to prevent -- and pandas over a raw psycopg2 connection
    infers different types than the DAG's SQLAlchemy engine, so the two writers
    disagreed on order_id and Spark refused the whole prefix:

      SchemaColumnConvertNotSupportedException: column: [order_id],
      physicalType: INT64, logicalType: int

    And the COPY into raw.orders_source moved the ingest DAG's high-water mark
    without leaving Bronze behind, which starved Pipe 2 of orders entirely
    (#144).

    One writer, then. Triggering the DAG also answers the original complaint in
    #149 -- that this step proved the pattern worked rather than that the
    pipeline did.
    """
    step("STEP 3 — Run the ingestion DAG over the seeded rows")

    cli = _airflow_cli()
    if cli is None:
        skip(
            "Could not reach Airflow to trigger ingest_source_to_bronze",
            "needs kubectl with the etl namespace, or a running docker-compose "
            "airflow-scheduler. Trigger by hand and re-run: "
            "airflow dags trigger ingest_source_to_bronze",
        )
        return

    if not _wait_for_dag(cli, "ingest_source_to_bronze"):
        return

    # The DAG is the only writer now, so checking its output is checking the
    # platform rather than this script's own arithmetic.
    dest = psycopg2.connect(**DEST)
    try:
        with dest.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.orders_source")
            rows = cur.fetchone()[0]
    finally:
        dest.close()
    if rows > 0:
        ok("Ingestion DAG loaded the warehouse", f"raw.orders_source holds {rows} rows")
    else:
        fail("raw.orders_source is empty after a successful ingestion run",
             "the DAG reported success but extracted nothing")



def simulate_transactions():
    step("STEP 4 — Simulate live transactions on postgres-source (UPDATE / DELETE)")

    conn = psycopg2.connect(**SOURCE)
    cur = conn.cursor()

    # Pick 200 existing order IDs
    cur.execute("SELECT order_id FROM orders ORDER BY order_id")
    all_ids = [r[0] for r in cur.fetchall()]

    # 40 status updates: pending → shipped / delivered
    update_ids = random.sample(all_ids, 40)
    for oid in update_ids:
        new_status = random.choice(["shipped", "delivered"])
        cur.execute(
            "UPDATE orders SET status=%s, updated_at=CURRENT_TIMESTAMP WHERE order_id=%s",
            (new_status, oid),
        )
    conn.commit()
    print(f"  → updated status on {len(update_ids)} orders")

    # 10 cancellations
    remaining = [i for i in all_ids if i not in update_ids]
    cancel_ids = random.sample(remaining, 10)
    for oid in cancel_ids:
        cur.execute(
            "UPDATE orders SET status='cancelled', updated_at=CURRENT_TIMESTAMP WHERE order_id=%s",
            (oid,),
        )
    conn.commit()
    print(f"  → cancelled {len(cancel_ids)} orders")

    # 5 hard deletes
    remaining2 = [i for i in remaining if i not in cancel_ids]
    delete_ids = random.sample(remaining2, 5)
    for oid in delete_ids:
        cur.execute("DELETE FROM order_items WHERE order_id=%s", (oid,))
        cur.execute("DELETE FROM orders WHERE order_id=%s", (oid,))
    conn.commit()
    print(f"  → deleted {len(delete_ids)} orders")

    cur.close()
    conn.close()

    ok("Transactions applied to source", "40 updates, 10 cancellations, 5 deletes")
    return {"updated": update_ids, "cancelled": cancel_ids, "deleted": delete_ids}


# ── Step 5: Verify the ClickHouse mirror (Pipe 3's real consumer) ─────────────
class ClickHouseAuthError(RuntimeError):
    """Wrong ClickHouse credentials — retrying will never help."""


def ch_query(sql):
    """Run a read-only query against ClickHouse, return list of rows."""
    r = requests.post(
        f"{CLICKHOUSE_URL}/?default_format=JSONCompact&readonly=1",
        headers={"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD},
        data=sql,
        timeout=15,
    )
    if r.status_code in (401, 403):
        raise ClickHouseAuthError(
            f"ClickHouse rejected user '{CLICKHOUSE_USER}' (HTTP {r.status_code}). "
            "Set CLICKHOUSE_USER/CLICKHOUSE_PASSWORD to match the deployment "
            "(scripts/test_e2e.sh reads them from the etl-secrets secret)."
        )
    r.raise_for_status()
    return r.json().get("data", [])


def verify_clickhouse(txn, max_order_id):
    step("STEP 5 — Verify ClickHouse mirror reflects all transactions")

    src = psycopg2.connect(**SOURCE)
    with src.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        src_count = cur.fetchone()[0]
    src.close()

    # The mirror retains rows from earlier seeds (dropped source tables emit
    # no CDC deletes), so every check is scoped to this run's order_id range.
    # All interpolated values below are ints generated by this test — the
    # ClickHouse HTTP interface has no parameter binding for ad-hoc queries.
    scope = f"order_id <= {max_order_id}"

    # Kafka -> ClickHouse ingestion is asynchronous; poll until counts settle
    deadline = time.time() + 90
    ch_count = -1
    while time.time() < deadline:
        try:
            ch_count = int(ch_query(f"SELECT count() FROM mirror.orders_current WHERE {scope}")[0][0])  # nosec B608
            if ch_count == src_count:
                break
        except ClickHouseAuthError as e:
            # Credentials will not fix themselves — stop instead of spending
            # 90 seconds printing the same rejection.
            fail("ClickHouse authentication failed", str(e))
            return
        except Exception as e:
            print(f"  waiting for ClickHouse… ({e})")
        time.sleep(5)

    if ch_count == src_count:
        ok("Row count matches", f"{ch_count} rows in both source and ClickHouse mirror")
    else:
        fail("Row count mismatch", f"source={src_count}, clickhouse={ch_count}")

    if ch_count <= 0:
        # Every check below looks for the *absence* of specific rows, which an
        # empty mirror satisfies trivially. Reporting those as passes hides the
        # only fact that matters: no CDC events arrived at all.
        fail(
            "Mirror is empty — downstream checks skipped",
            "no CDC events reached ClickHouse, so delete and cancellation "
            "verification would pass vacuously. Check the Debezium connector "
            "task state and the ClickHouse Kafka consumer.",
        )
        return

    del_ids = ",".join(str(i) for i in txn["deleted"])
    found = int(ch_query(f"SELECT count() FROM mirror.orders_current WHERE order_id IN ({del_ids})")[0][0])  # nosec B608
    if found == 0:
        ok(f"All {len(txn['deleted'])} deleted orders are gone from the mirror")
    else:
        fail(f"{found} deleted orders still present in the mirror")

    cancel_ids = ",".join(str(i) for i in txn["cancelled"])
    matched = int(ch_query(
        f"SELECT count() FROM mirror.orders_current WHERE order_id IN ({cancel_ids}) AND status='cancelled'"  # nosec B608
    )[0][0])
    if matched == len(txn["cancelled"]):
        ok(f"All {len(txn['cancelled'])} cancellations reflected in the mirror")
    else:
        fail("Cancellation mismatch", f"expected {len(txn['cancelled'])}, got {matched}")

    rows = ch_query(
        f"SELECT status, count() FROM mirror.orders_current WHERE {scope} GROUP BY status ORDER BY count() DESC"  # nosec B608
    )
    print("\n  Status distribution in ClickHouse mirror:")
    print(f"  {'Status':<15} {'Count':>6}")
    print(f"  {'-'*22}")
    for status, count in rows:
        print(f"  {status:<15} {count:>6}")


DAG_WAIT_SECONDS = int(os.environ.get("SILVER_DAG_TIMEOUT", "900"))


def _airflow_cli():
    """How to reach the Airflow CLI here, or None.

    Checked in the order the platform is usually run: a Kubernetes cluster with
    the etl namespace, then docker-compose. Returns the argv prefix that a dag
    subcommand is appended to.
    """
    if shutil.which("kubectl"):
        probe = subprocess.run(  # nosec B603 B607 - fixed argv, no shell
            ["kubectl", "get", "deploy/airflow-scheduler", "-n", "etl"],
            capture_output=True, text=True,
        )
        if probe.returncode == 0:
            return ["kubectl", "exec", "-n", "etl", "deploy/airflow-scheduler",
                    "--", "airflow"]
    if shutil.which("docker"):
        probe = subprocess.run(  # nosec B603 B607 - fixed argv, no shell
            ["docker", "compose", "ps", "-q", "airflow-scheduler"],
            capture_output=True, text=True,
        )
        if probe.returncode == 0 and probe.stdout.strip():
            return ["docker", "compose", "exec", "-T", "airflow-scheduler",
                    "airflow"]
    return None


def _runs(cli, dag_id):
    """Every run of `dag_id`, newest first, or None."""
    out = subprocess.run(  # nosec B603 - fixed argv, no shell
        # dag_id is positional in Airflow 3; "-d" makes the CLI print help and
        # exit 2, which read as "no runs" rather than as a broken command.
        cli + ["dags", "list-runs", dag_id, "--no-backfill", "-o", "json"],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        return None
    # Airflow writes its own log lines to stdout ahead of the JSON, and they
    # contain brackets -- "[error    ] Could not configure StatsClient: [Errno
    # -2]". Slicing from the first "[" parsed that instead of the payload.
    start = out.stdout.find("[{")
    if start == -1:
        start = out.stdout.find("[]")
    if start == -1:
        return None
    try:
        return json.loads(out.stdout[start:])
    except ValueError:
        return None


def _stuck_detail(outstanding):
    """Say whether the queue is merely deep or wedged behind a zombie run.

    A backlog and a run stuck in "running" since last week look identical from
    the outside -- both report "queued" forever -- but the remedies are
    opposite: wait longer, versus go and kill something. #144 spent a round trip
    on that, because the message only offered "raise the timeout" while the real
    blocker was a run that had been running for nine days.
    """
    kinds = ', '.join(sorted({r['state'] for r in outstanding}))
    oldest = min((r for r in outstanding if r.get('start_date')),
                 key=lambda r: r['start_date'], default=None)
    detail = f"{len(outstanding)} run(s) {kinds}. "
    if oldest:
        started = oldest['start_date'][:19]
        age_days = 0
        try:
            started_at = datetime.fromisoformat(oldest['start_date'].replace('Z', '+00:00'))
            age_days = (datetime.now(started_at.tzinfo) - started_at).days
        except ValueError:
            pass
        if age_days >= 1:
            return (detail + f"The oldest has been running since {started} "
                    f"({age_days} day(s)) -- that is a stuck run holding the "
                    f"only active slot (max_active_runs=1), not a backlog. Mark "
                    f"it failed in the Airflow UI; deleting it often errors "
                    f"while it is still active.")
        detail += f"Oldest started {started}. "
    return (detail + "A backlog drains one run at a time (max_active_runs=1); "
            "raise SILVER_DAG_TIMEOUT, or clear old runs in the Airflow UI.")


def _wait_for_dag(cli, dag_id):
    """Trigger `dag_id` unless already busy, then wait. True only on success.

    Never adds to the queue when something is pending. These DAGs run
    max_active_runs=1, so a new run joins the BACK of whatever is waiting and
    the wait then covers the whole backlog -- which is how a healthy cluster
    reported "did not finish within 900s (last state: queued)". A run already
    queued or running does the same work.
    """
    pending = [r for r in (_runs(cli, dag_id) or [])
               if r.get("state") in ("queued", "running")]
    if pending:
        print(f"  {len(pending)} {dag_id} run(s) already queued or running; "
              f"waiting for those rather than adding another")
    else:
        trigger = subprocess.run(  # nosec B603 - fixed argv, no shell
            cli + ["dags", "trigger", dag_id], capture_output=True, text=True,
        )
        if trigger.returncode != 0:
            fail(f"Could not trigger {dag_id}",
                 (trigger.stderr or trigger.stdout).strip()[:200])
            return False
        print(f"  triggered {dag_id}")
    print(f"  waiting up to {DAG_WAIT_SECONDS}s for the queue to drain "
          f"(SILVER_DAG_TIMEOUT to change)")

    deadline = time.time() + DAG_WAIT_SECONDS
    runs = []
    while time.time() < deadline:
        time.sleep(15)
        runs = _runs(cli, dag_id) or []
        outstanding = [r for r in runs if r.get("state") in ("queued", "running")]
        if not outstanding:
            break
        print(f"    {len(outstanding)} outstanding "
              f"({', '.join(sorted({r['state'] for r in outstanding}))})...")

    outstanding = [r for r in runs if r.get("state") in ("queued", "running")]
    if outstanding:
        fail(f"{dag_id} still busy after {DAG_WAIT_SECONDS}s", _stuck_detail(outstanding))
    elif runs and runs[0].get("state") == "success":
        ok(f"{dag_id} completed")
        return True
    elif runs and runs[0].get("state") == "failed":
        fail(f"{dag_id} failed",
             "open the red task in the Airflow UI; for the silver DAG the line "
             "that matters is 'Loaded N records from Bronze'")
    else:
        fail(f"Could not read {dag_id} run state", f"last seen: {runs[:1] or 'nothing'}")
    return False


def run_silver_pipeline():
    """Run the Spark silver pipeline over the Bronze the ingestion DAG wrote."""
    step("STEP 6 — Run the Spark silver pipeline over the Bronze just written")

    cli = _airflow_cli()
    if cli is None:
        skip(
            "Could not reach Airflow to trigger spark_transform_silver",
            "needs kubectl with the etl namespace, or a running docker-compose "
            "airflow-scheduler. Trigger by hand and re-run: "
            "airflow dags trigger spark_transform_silver",
        )
        return
    _wait_for_dag(cli, "spark_transform_silver")



# ── Step 6: Lakehouse (Pipe 2) ────────────────────────────────────────────────
def trino_query(sql):
    """Run one statement over Trino's REST protocol, following nextUri.

    Results arrive across pages and the first page usually carries no rows at
    all, so a naive single POST reads an empty result and calls it a passing
    zero. Follow the chain to the end.
    """
    r = requests.post(
        f"{TRINO_URL}/v1/statement",
        data=sql.encode("utf-8"),
        headers={"X-Trino-User": "e2e-test"},
        timeout=30,
    )
    rows = []
    while True:
        r.raise_for_status()
        page = r.json()
        if page.get("error"):
            raise RuntimeError(page["error"].get("message", "unknown Trino error"))
        rows.extend(page.get("data") or [])
        nxt = page.get("nextUri")
        if not nxt:
            return rows
        r = requests.get(nxt, headers={"X-Trino-User": "e2e-test"}, timeout=30)


def verify_lakehouse():
    step("STEP 7 — Verify the Iceberg lakehouse (Pipe 2) through Trino")

    if not TRINO_URL:
        skip(
            "Lakehouse not checked — TRINO_URL unset",
            "set TRINO_URL (test_e2e.sh port-forwards it) to verify Pipe 2",
        )
        return

    try:
        tables = {r[0] for r in trino_query(
            "SELECT table_name FROM iceberg.information_schema.tables "
            "WHERE table_schema = 'lake'"
        )}
    except Exception as e:
        fail("Trino is not answering", str(e)[:200])
        return

    if "orders" not in tables:
        fail(
            "iceberg.lake.orders does not exist",
            "the Spark silver job has never completed. Check the "
            "spark_transform_silver DAG, and that the iceberg_catalog schema "
            "exists in postgres-dest",
        )
        return
    ok("Iceberg tables present", ", ".join(sorted(tables)))

    try:
        lake_count = int(trino_query("SELECT count(*) FROM iceberg.lake.orders")[0][0])
    except Exception as e:
        fail("Could not read iceberg.lake.orders", str(e)[:200])
        return

    if lake_count > 0:
        ok("Lakehouse is readable and populated", f"{lake_count} rows in iceberg.lake.orders")
    else:
        fail(
            "iceberg.lake.orders is empty",
            "the table exists but no Spark run has merged bronze into it",
        )
        return

    # Deliberately not asserted equal to the source. Pipe 2 is batch, so it
    # trails by up to one run -- and the MERGE never deletes, so the five rows
    # this test hard-deletes from the source stay in the lake for good. A test
    # demanding equality here would fail correctly-behaving infrastructure.
    src = psycopg2.connect(**SOURCE)
    with src.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        src_count = cur.fetchone()[0]
    src.close()
    print("")
    print(f"  source={src_count}  lake={lake_count}  "
          "(batch lag plus merge-only semantics: the lake keeps deleted rows)")


# ── Step 8: Warehouse marts (Pipe 1) ──────────────────────────────────────────
def verify_warehouse_marts():
    step("STEP 8 — Verify the dbt marts (Pipe 1) are built and populated")

    # Step 3 writes raw.orders_source itself, so finding rows there proves
    # nothing about the DAG. int/gold can only exist if the real dbt task
    # group ran, which makes them the honest signal for Pipe 1.
    expected = [
        ("int", "orders_clean"),
        ("gold", "fact_order_items"),
        ("gold", "dim_customer"),
        ("gold", "dim_date"),
    ]
    dest = psycopg2.connect(**DEST)
    try:
        with dest.cursor() as cur:
            cur.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema IN ('int', 'gold')"
            )
            present = {(r[0], r[1]) for r in cur.fetchall()}

            missing = [f"{sc}.{tb}" for sc, tb in expected if (sc, tb) not in present]
            if missing:
                fail(
                    "dbt marts missing: " + ", ".join(missing),
                    "the dbt task group in ingest_source_to_bronze has not run "
                    "successfully. raw.* being populated says nothing here, "
                    "because step 3 of this test writes it directly",
                )
                return
            ok("dbt marts present", f"{len(expected)} of {len(expected)} in int/gold")

            empty = []
            for schema, table in expected:
                # identifiers come from the list above, never from input
                cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')  # nosec B608
                if cur.fetchone()[0] == 0:
                    empty.append(f"{schema}.{table}")
            if empty:
                fail(
                    "dbt marts built but empty: " + ", ".join(empty),
                    "dbt ran against a raw layer it could not read, or the "
                    "quality filters in the int models dropped every row",
                )
            else:
                ok("dbt marts populated", "int and gold both hold rows")
    finally:
        dest.close()


# ── Step 9: Summary ───────────────────────────────────────────────────────────
def summary():
    step("STEP 9 — Test Summary")
    passed = sum(1 for r in results if r[0] == "PASS")
    failed = sum(1 for r in results if r[0] == "FAIL")
    skipped = sum(1 for r in results if r[0] == "SKIP")
    icons = {"PASS": "✓", "FAIL": "✗", "SKIP": "~"}
    for status, msg in results:
        print(f"  {icons[status]}  {msg}")
    print("")
    print(f"  Total: {passed} passed, {failed} failed, {skipped} skipped")

    if failed:
        print("")
        print("  Some checks failed — review the output above for details.")
        sys.exit(1)

    # A green run has to say what it actually covered. Reporting "pipeline is
    # healthy" off a suite that never read the lakehouse is the whole of #149,
    # and it held for weeks while Pipe 2 had no successful Trino read at all.
    print("")
    if skipped:
        print(f"  {passed} passed, but {skipped} skipped — the "
              f"pipelines behind them are UNVERIFIED, not healthy.")
        print("  Configure the endpoints named above and re-run to cover all three.")
    else:
        print("  All checks passed — all three pipelines verified end to end:")
        print("    Pipe 1  warehouse: dbt int/gold marts built and populated")
        print("    Pipe 2  lakehouse: iceberg.lake.orders readable through Trino")
        print("    Pipe 3  mirror:    counts, deletes and cancellations in ClickHouse")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*60)
    print("  ETL Transactional Test Suite")
    print("="*60)

    seed_source()
    register_connector()

    # Give Debezium ~5s to take the initial snapshot into Kafka
    print("\n  Waiting 5s for Debezium to snapshot initial data…")
    time.sleep(5)

    run_extract_pipeline()
    txn = simulate_transactions()

    verify_clickhouse(txn, max_order_id=200)
    run_silver_pipeline()
    verify_lakehouse()
    verify_warehouse_marts()
    summary()
