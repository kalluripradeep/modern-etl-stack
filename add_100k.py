import sys
import psycopg2
sys.path.append('sample-data')
from generate_ecommerce import DB_CONFIG, generate_orders, print_stats

print('Connecting to DB...')
conn = psycopg2.connect(**DB_CONFIG)
print('Appending 100,000 orders...')
generate_orders(conn, count=100000)
print_stats(conn)
conn.close()
