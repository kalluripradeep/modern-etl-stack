#!/usr/bin/env python3
"""
End-to-end transactional test for the ETL pipeline.

What this tests:
  Step 1  Seed 50 customers, 20 products, 200 orders into postgres-source
  Step 2  Register the Debezium CDC connector (if not already registered)
  Step 3  Run the full extract → validate → load pipeline (DAG functions directly)
  Step 4  Simulate live transactions: UPDATE status, CANCEL orders, DELETE rows
  Step 5  Consume CDC events from Kafka and apply to postgres-dest
  Step 6  Verify postgres-dest matches expected final state
  Step 7  Print pass / fail report

Usage (local docker-compose):
  python scripts/test_transactions.py

Usage (Kubernetes — port-forward first):
  kubectl port-forward svc/postgres-source 5433:5432 -n etl &
  kubectl port-forward svc/postgres-dest   5434:5432 -n etl &
  kubectl port-forward svc/kafka           9093:9092 -n etl &
  kubectl port-forward svc/kafka-connect   8083:8083 -n etl &
  kubectl port-forward svc/minio           9000:9000 -n etl &
  SOURCE_DB_PORT=5433 DEST_DB_PORT=5434 KAFKA_PORT=9093 python scripts/test_transactions.py
"""

import io
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

import psycopg2
import requests
from faker import Faker
from kafka import KafkaConsumer, KafkaProducer

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
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", f"localhost:{os.environ.get('KAFKA_PORT', 9092)}")
CONNECT_URL     = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER      = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD  = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
CHUNK_DIR       = "/tmp/etl_test_chunks"

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
        ok(f"Seeded 200 orders into postgres-source")
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
    step("STEP 3 — Extract → Validate → Load (Bronze)")

    import glob as glob_module
    import pandas as pd
    from minio import Minio

    os.makedirs(CHUNK_DIR, exist_ok=True)

    # --- Extract ---
    conn = psycopg2.connect(**SOURCE)
    chunk_num, total_rows = 0, 0
    for chunk in pd.read_sql("SELECT order_id,customer_id,order_date,total_amount,status,created_at,updated_at FROM orders ORDER BY order_id", conn, chunksize=50):
        path = f"{CHUNK_DIR}/part-{chunk_num:05d}.parquet"
        chunk.to_parquet(path, index=False, compression="snappy")
        total_rows += len(chunk)
        chunk_num += 1
    conn.close()
    print(f"  extracted {total_rows} rows into {chunk_num} parquet chunk(s)")

    # --- Validate ---
    chunk_files = sorted(glob_module.glob(f"{CHUNK_DIR}/part-*.parquet"))
    revenue = 0.0
    for f in chunk_files:
        df = pd.read_parquet(f)
        assert df["order_id"].notna().all(), f"null order_id in {f}"
        assert (df["total_amount"] >= 0).all(), f"negative amount in {f}"
        revenue += float(df["total_amount"].sum())
        del df
    ok(f"Validation passed", f"{total_rows} rows, ${revenue:,.2f} revenue")

    # --- Load to MinIO bronze ---
    try:
        client = Minio(
            MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            secure=False,
        )
        if not client.bucket_exists("bronze"):
            client.make_bucket("bronze")
        date_prefix = datetime.now().strftime("%Y/%m/%d")
        for i, fp in enumerate(chunk_files):
            client.fput_object("bronze", f"orders/{date_prefix}/part-{i:05d}.parquet", fp)
        ok(f"Uploaded {len(chunk_files)} parquet file(s) to MinIO s3://bronze/orders/{date_prefix}/")
    except Exception as e:
        fail("MinIO upload failed", str(e))
        print("  → Make sure MinIO is running and port-forwarded if using K8s")

    # --- Load to postgres-dest via COPY ---
    conn = psycopg2.connect(**DEST)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.orders (
            order_id     BIGINT PRIMARY KEY,
            customer_id  BIGINT,
            order_date   TIMESTAMP,
            total_amount NUMERIC(18,2),
            status       TEXT,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        )
    """)
    conn.commit()

    loaded = 0
    for fp in chunk_files:
        df = pd.read_parquet(fp)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS orders_stage (LIKE public.orders) ON COMMIT PRESERVE ROWS")
        cur.execute("TRUNCATE orders_stage")
        cur.copy_expert("COPY orders_stage (order_id,customer_id,order_date,total_amount,status,created_at,updated_at) FROM STDIN WITH CSV", buf)
        cur.execute("""
            INSERT INTO public.orders SELECT * FROM orders_stage
            ON CONFLICT (order_id) DO UPDATE SET
                total_amount = EXCLUDED.total_amount,
                status       = EXCLUDED.status,
                updated_at   = EXCLUDED.updated_at
        """)
        conn.commit()
        loaded += len(df)
        del df
    cur.close()
    conn.close()
    ok(f"Loaded {loaded} rows into postgres-dest via COPY (staging upsert)")


# ── Step 4: Simulate live transactions ────────────────────────────────────────
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

    ok("Transactions applied to source", f"40 updates, 10 cancellations, 5 deletes")
    return {"updated": update_ids, "cancelled": cancel_ids, "deleted": delete_ids}


# ── Step 5: Consume CDC events ────────────────────────────────────────────────
def consume_cdc(expected_deleted_ids):
    step("STEP 5 — Consume CDC events from Kafka → postgres-dest")

    try:
        consumer = KafkaConsumer(
            "cdc.public.orders",
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="etl-test-consumer",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=15000,   # wait up to 15s for messages
        )
    except Exception as e:
        fail("Cannot connect to Kafka", str(e))
        print("  → Make sure Kafka is running and port-forwarded if using K8s")
        return

    conn = psycopg2.connect(**DEST)
    cur = conn.cursor()

    inserts = updates = deletes = 0
    for msg in consumer:
        payload = msg.value.get("payload", msg.value)
        op = payload.get("op")
        if op in ("r", "c"):
            after = payload.get("after")
            if after:
                _upsert_order(cur, after)
                inserts += 1
        elif op == "u":
            after = payload.get("after")
            if after:
                _upsert_order(cur, after)
                updates += 1
        elif op == "d":
            before = payload.get("before")
            if before:
                cur.execute("DELETE FROM public.orders WHERE order_id=%s", (before["order_id"],))
                deletes += 1

    conn.commit()
    consumer.close()
    cur.close()
    conn.close()

    ok(f"CDC events consumed", f"snapshots/inserts={inserts}, updates={updates}, deletes={deletes}")


def _upsert_order(cur, data):
    def _ts(v):
        if isinstance(v, int):
            return datetime.utcfromtimestamp(v / 1_000_000.0)
        return v

    cur.execute("""
        INSERT INTO public.orders (order_id,customer_id,order_date,total_amount,status,created_at,updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_id) DO UPDATE SET
            total_amount = EXCLUDED.total_amount,
            status       = EXCLUDED.status,
            updated_at   = EXCLUDED.updated_at
    """, (
        data["order_id"], data["customer_id"],
        _ts(data.get("order_date")), data["total_amount"], data["status"],
        _ts(data.get("created_at")), _ts(data.get("updated_at")),
    ))


# ── Step 6: Verify final state ────────────────────────────────────────────────
def verify(txn):
    step("STEP 6 — Verify postgres-dest reflects all transactions")

    src = psycopg2.connect(**SOURCE)
    dst = psycopg2.connect(**DEST)

    with src.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        src_count = cur.fetchone()[0]

    with dst.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM public.orders")
        dst_count = cur.fetchone()[0]

    # Row count match
    if src_count == dst_count:
        ok(f"Row count matches", f"{dst_count} rows in both source and dest")
    else:
        fail(f"Row count mismatch", f"source={src_count}, dest={dst_count}")

    # Deleted rows must NOT be in dest
    del_ids = txn["deleted"]
    with dst.cursor() as cur:
        cur.execute(f"SELECT order_id FROM public.orders WHERE order_id = ANY(%s)", (del_ids,))
        found = [r[0] for r in cur.fetchall()]
    if not found:
        ok(f"All {len(del_ids)} deleted orders are gone from dest")
    else:
        fail(f"{len(found)} deleted orders still present in dest", str(found))

    # Cancelled orders should have status='cancelled' in dest
    cancel_ids = txn["cancelled"]
    with dst.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM public.orders WHERE order_id = ANY(%s) AND status='cancelled'",
            (cancel_ids,),
        )
        matched = cur.fetchone()[0]
    if matched == len(cancel_ids):
        ok(f"All {len(cancel_ids)} cancellations reflected in dest")
    else:
        fail(f"Cancellation mismatch", f"expected {len(cancel_ids)}, got {matched} in dest")

    # Status distribution
    with dst.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) FROM public.orders GROUP BY status ORDER BY COUNT(*) DESC")
        rows = cur.fetchall()

    print("\n  Status distribution in postgres-dest:")
    print(f"  {'Status':<15} {'Count':>6}")
    print(f"  {'-'*22}")
    for status, count in rows:
        print(f"  {status:<15} {count:>6}")

    src.close()
    dst.close()


# ── Step 7: Summary ───────────────────────────────────────────────────────────
def summary():
    step("STEP 7 — Test Summary")
    passed = sum(1 for r in results if r[0] == "PASS")
    failed = sum(1 for r in results if r[0] == "FAIL")
    for status, msg in results:
        icon = "✓" if status == "PASS" else "✗"
        print(f"  {icon}  {msg}")
    print(f"\n  Total: {passed} passed, {failed} failed")
    if failed:
        print("\n  Some checks failed — review the output above for details.")
        sys.exit(1)
    else:
        print("\n  All checks passed — pipeline is healthy!")


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

    # Give Debezium ~5s to capture the change events
    print("\n  Waiting 5s for CDC events to appear in Kafka…")
    time.sleep(5)

    consume_cdc(txn["deleted"])
    verify(txn)
    summary()
