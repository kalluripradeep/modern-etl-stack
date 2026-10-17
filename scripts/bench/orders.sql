-- Continuous-load CDC workload for pgbench.
--
-- Mirrors the event mix of scripts/simulate_live_traffic.py -- one order
-- INSERT, one line item, one order UPDATE -- so throughput and lag numbers
-- stay comparable between the two.
--
-- The difference that matters is the commit boundary. The tick generator
-- writes a whole tick in one transaction, so at high rates a single tick can
-- stay open for over a minute and Debezium sees nothing until it commits.
-- Freshness then measures transaction duration rather than pipeline lag, and
-- draws a sawtooth that looks like backpressure on an idle source. pgbench
-- wraps each script run in its own transaction, so every order commits
-- separately and freshness means what it is supposed to mean.
--
--   docker cp scripts/bench/orders.sql postgres-source:/tmp/orders.sql
--   docker exec -e PGPASSWORD=sourcepass postgres-source pgbench -h 127.0.0.1 -U sourceuser -d sourcedb -f /tmp/orders.sql -c 16 -j 4 -T 100 -P 10 --no-vacuum
--
-- On Kubernetes the password is generated, so read it from the secret:
--
--   PGPW=$(kubectl get secret etl-secrets -n etl -o jsonpath='{.data.SOURCE_DB_PASSWORD}' | base64 -d)
--   kubectl cp scripts/bench/orders.sql etl/postgres-source-0:/tmp/orders.sql
--   kubectl exec -n etl postgres-source-0 -- env PGPASSWORD="$PGPW" pgbench -h 127.0.0.1 -U sourceuser -d sourcedb -f /tmp/orders.sql -c 16 -j 4 -T 100 -P 10 --no-vacuum
--
-- Add -R <rate> to hold a fixed rate instead of pushing to saturation, which
-- is what you want when comparing lag between runs rather than finding the
-- ceiling. pgbench ships in the postgres:15 image, so there is nothing to
-- install in either place.
--
-- Rows are picked with ORDER BY random() rather than random(1, N) because the
-- sequences do not reliably start at 1 -- the source gets reseeded, and a
-- hardcoded range quietly becomes foreign key violations. customers and
-- products are small enough that the sort is noise next to the write.
SELECT customer_id FROM customers ORDER BY random() LIMIT 1 \gset
SELECT product_id, price FROM products ORDER BY random() LIMIT 1 \gset
\set qty random(1, 3)
INSERT INTO orders (customer_id, order_date, total_amount, status)
  VALUES (:customer_id, CURRENT_TIMESTAMP, 0, 'pending')
  RETURNING order_id \gset
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
  VALUES (:order_id, :product_id, :qty, :price);
UPDATE orders SET total_amount = :qty * :price, updated_at = CURRENT_TIMESTAMP
  WHERE order_id = :order_id;
