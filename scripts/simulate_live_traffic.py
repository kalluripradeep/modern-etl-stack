"""Continuously generate live traffic on the source database.

Unlike sample-data/generate_ecommerce.py (which DROPs and recreates the
tables, breaking CDC continuity), this only ever appends and updates, so
the Debezium slot, the ClickHouse mirror and the batch high-water mark all
stay valid while it runs. Use it to watch records flow through all three
pipelines end to end.

Each tick places a few new orders (with line items), advances the status of
some existing ones, and occasionally cancels or deletes one — enough to
exercise inserts, updates and deletes in the CDC stream.

Usage (from the repo root):
    python scripts/simulate_live_traffic.py                  # 3 orders every 5s, forever
    python scripts/simulate_live_traffic.py --rate 10 --interval 2
    python scripts/simulate_live_traffic.py --duration 15    # stop after 15 minutes
    python scripts/simulate_live_traffic.py --once           # a single tick, then exit

Against Kubernetes, port-forward the source first:
    kubectl port-forward svc/postgres-source 5432:5432 -n etl
"""

import argparse
import os
import random
import signal
import sys
import time
from datetime import datetime

import psycopg2

SOURCE = {
    'host': os.environ.get('SOURCE_DB_HOST', 'localhost'),
    'port': int(os.environ.get('SOURCE_DB_PORT', 5432)),
    'database': os.environ.get('SOURCE_DB_NAME', 'sourcedb'),
    'user': os.environ.get('SOURCE_DB_USER', 'sourceuser'),
    'password': os.environ.get('SOURCE_DB_PASSWORD', 'sourcepass'),
}

STATUSES = ['pending', 'processing', 'shipped', 'delivered']
_running = True


def _stop(signum, frame):
    global _running
    _running = False
    print("\n  stopping after this tick…")


# What this script writes. A source database seeded by an older version of
# sample-data/generate_ecommerce.py can be missing some of these — order_items
# used to carry `price` rather than `unit_price` — and the insert then fails
# several ticks in with a bare psycopg2 UndefinedColumn, naming one column and
# offering no remedy.
REQUIRED_COLUMNS = {
    'orders': {'order_id', 'customer_id', 'order_date', 'total_amount',
               'status', 'updated_at'},
    'order_items': {'order_id', 'product_id', 'quantity', 'unit_price'},
}


def check_schema(cur):
    """Fail early and legibly if the source predates the current schema."""
    problems = []
    for table, needed in REQUIRED_COLUMNS.items():
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        present = {r[0] for r in cur.fetchall()}
        if not present:
            problems.append(f"  {table}: table does not exist")
            continue
        missing = needed - present
        if missing:
            problems.append(f"  {table}: missing {', '.join(sorted(missing))}"
                            f"  (has: {', '.join(sorted(present))})")
    if problems:
        sys.exit(
            "Source schema does not match what this script writes:\n"
            + "\n".join(problems)
            + "\n\nThis usually means the database was seeded by an older "
              "version of sample-data/generate_ecommerce.py — order_items "
              "carried 'price' before it was renamed to 'unit_price'.\n"
              "Re-seed to bring it in line (this drops and recreates the "
              "source tables):\n"
              "  make seed\n"
              "or, on Kubernetes, re-run bash k8s/deploy.sh and answer y when "
              "it offers to seed."
        )


def load_reference_data(cur):
    """Existing customers and products to reference — keeps FKs valid."""
    cur.execute("SELECT customer_id FROM customers")
    customers = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products")
    products = cur.fetchall()
    if not customers or not products:
        sys.exit("No customers/products in the source. Run `make seed` once first.")
    return customers, products


def place_orders(cur, customers, products, count):
    """Insert `count` new orders, each with 1-3 line items."""
    for _ in range(count):
        cur.execute(
            "INSERT INTO orders (customer_id, order_date, total_amount, status) "
            "VALUES (%s, CURRENT_TIMESTAMP, 0, 'pending') RETURNING order_id",
            (random.choice(customers),),
        )
        order_id = cur.fetchone()[0]

        total = 0
        for _ in range(random.randint(1, 3)):
            product_id, price = random.choice(products)
            qty = random.randint(1, 3)
            total += float(price) * qty
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                "VALUES (%s, %s, %s, %s)",
                (order_id, product_id, qty, price),
            )

        # Keep the order total equal to the sum of its lines, so the gold
        # layer's fact/order reconciliation stays exact.
        cur.execute(
            "UPDATE orders SET total_amount = %s, updated_at = CURRENT_TIMESTAMP "
            "WHERE order_id = %s",
            (round(total, 2), order_id),
        )
    return count


def advance_statuses(cur, count):
    """Move some recent orders along their lifecycle (UPDATE events)."""
    cur.execute(
        "SELECT order_id FROM orders WHERE status <> 'cancelled' "
        "ORDER BY order_id DESC LIMIT 200"
    )
    ids = [r[0] for r in cur.fetchall()]
    if not ids:
        return 0
    for oid in random.sample(ids, min(count, len(ids))):
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = CURRENT_TIMESTAMP "
            "WHERE order_id = %s",
            (random.choice(STATUSES), oid),
        )
    return min(count, len(ids))


def cancel_order(cur):
    """Cancel one recent order (UPDATE event)."""
    cur.execute(
        "SELECT order_id FROM orders WHERE status <> 'cancelled' "
        "ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return 0
    cur.execute(
        "UPDATE orders SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP "
        "WHERE order_id = %s",
        (row[0],),
    )
    return 1


def delete_order(cur):
    """Hard-delete one cancelled order (DELETE event — tests tombstones)."""
    cur.execute(
        "SELECT order_id FROM orders WHERE status = 'cancelled' "
        "ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return 0
    cur.execute("DELETE FROM order_items WHERE order_id = %s", (row[0],))
    cur.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))
    return 1


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--rate', type=int, default=3, help='new orders per tick (default 3)')
    p.add_argument('--interval', type=float, default=5.0, help='seconds between ticks (default 5)')
    p.add_argument('--duration', type=float, default=0, help='minutes to run; 0 = until Ctrl-C')
    p.add_argument('--once', action='store_true', help='run a single tick and exit')
    args = p.parse_args()

    signal.signal(signal.SIGINT, _stop)

    conn = psycopg2.connect(**SOURCE)
    conn.autocommit = True
    cur = conn.cursor()
    check_schema(cur)
    customers, products = load_reference_data(cur)

    print(f"Live traffic → {SOURCE['host']}:{SOURCE['port']}/{SOURCE['database']}")
    print(f"  {args.rate} new order(s) every {args.interval}s "
          f"({'single tick' if args.once else 'Ctrl-C to stop'})\n")

    deadline = time.time() + args.duration * 60 if args.duration else None
    placed = updated = cancelled = deleted = ticks = 0

    while _running:
        ticks += 1
        placed += place_orders(cur, customers, products, args.rate)
        updated += advance_statuses(cur, max(1, args.rate // 2))
        if ticks % 4 == 0:
            cancelled += cancel_order(cur)
        if ticks % 10 == 0:
            deleted += delete_order(cur)

        cur.execute("SELECT count(*) FROM orders")
        total_orders = cur.fetchone()[0]
        print(f"  [{datetime.now():%H:%M:%S}] tick {ticks:>4} | "
              f"+{placed} placed  ~{updated} updated  "
              f"x{cancelled} cancelled  -{deleted} deleted | "
              f"source now holds {total_orders} orders")

        if args.once or (deadline and time.time() >= deadline):
            break
        time.sleep(args.interval)

    cur.close()
    conn.close()
    print(f"\nDone. {placed} placed, {updated} updated, "
          f"{cancelled} cancelled, {deleted} deleted across {ticks} tick(s).")


if __name__ == '__main__':
    main()
