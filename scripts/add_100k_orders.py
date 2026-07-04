"""Append 100,000 extra orders to the source database for load testing.

Usage (from the repo root): python scripts/add_100k_orders.py
"""

import os
import sys

import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'sample-data'))
from generate_ecommerce import DB_CONFIG, generate_orders, print_stats  # noqa: E402

print('Connecting to DB...')
conn = psycopg2.connect(**DB_CONFIG)
print('Appending 100,000 orders...')
generate_orders(conn, count=100000)
print_stats(conn)
conn.close()
