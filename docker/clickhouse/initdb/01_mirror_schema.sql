-- Columnar hot mirror fed from the Debezium CDC topics.
--
-- Pattern per table:
--   kafka_<t>   Kafka engine table reading cdc.public.<t> as raw JSON
--   <t>         ReplacingMergeTree(ver, is_deleted) — latest row version wins,
--               deletes tombstone via is_deleted
--   <t>_mv      materialized view parsing the Debezium envelope
--   <t>_current clean read view (collapsed, deletes filtered)
--
-- Runs on first boot of a fresh ClickHouse volume. For an existing volume:
--   docker exec -i clickhouse clickhouse-client --multiquery < docker/clickhouse/initdb/01_mirror_schema.sql

CREATE DATABASE IF NOT EXISTS mirror;

-- ── orders ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mirror.orders
(
    order_id     Int64,
    customer_id  Int64,
    order_date   DateTime64(6),
    total_amount Float64,
    status       LowCardinality(String),
    created_at   DateTime64(6),
    updated_at   DateTime64(6),
    ver          UInt64,
    is_deleted   UInt8
)
ENGINE = ReplacingMergeTree(ver, is_deleted)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS mirror.kafka_orders (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'cdc.public.orders',
         kafka_group_name = 'clickhouse-mirror-orders',
         kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS mirror.orders_mv TO mirror.orders AS
WITH
    JSONExtractString(raw, 'op') AS op,
    if(op = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS rec
SELECT
    JSONExtractInt(rec, 'order_id')                          AS order_id,
    JSONExtractInt(rec, 'customer_id')                       AS customer_id,
    toDateTime64(JSONExtractInt(rec, 'order_date') / 1000000, 6)  AS order_date,
    JSONExtractFloat(rec, 'total_amount')                    AS total_amount,
    JSONExtractString(rec, 'status')                         AS status,
    toDateTime64(JSONExtractInt(rec, 'created_at') / 1000000, 6)  AS created_at,
    toDateTime64(JSONExtractInt(rec, 'updated_at') / 1000000, 6)  AS updated_at,
    JSONExtractUInt(raw, 'ts_ms')                            AS ver,
    if(op = 'd', 1, 0)                                       AS is_deleted
FROM mirror.kafka_orders
WHERE length(raw) > 0 AND rec NOT IN ('', 'null');

CREATE VIEW IF NOT EXISTS mirror.orders_current AS
SELECT * EXCEPT (ver, is_deleted) FROM mirror.orders FINAL WHERE is_deleted = 0;

-- ── customers ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mirror.customers
(
    customer_id Int64,
    first_name  String,
    last_name   String,
    email       String,
    address     String,
    city        String,
    state       String,
    zip_code    String,
    created_at  DateTime64(6),
    updated_at  DateTime64(6),
    ver         UInt64,
    is_deleted  UInt8
)
ENGINE = ReplacingMergeTree(ver, is_deleted)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS mirror.kafka_customers (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'cdc.public.customers',
         kafka_group_name = 'clickhouse-mirror-customers',
         kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS mirror.customers_mv TO mirror.customers AS
WITH
    JSONExtractString(raw, 'op') AS op,
    if(op = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS rec
SELECT
    JSONExtractInt(rec, 'customer_id')                       AS customer_id,
    JSONExtractString(rec, 'first_name')                     AS first_name,
    JSONExtractString(rec, 'last_name')                      AS last_name,
    JSONExtractString(rec, 'email')                          AS email,
    JSONExtractString(rec, 'address')                        AS address,
    JSONExtractString(rec, 'city')                           AS city,
    JSONExtractString(rec, 'state')                          AS state,
    JSONExtractString(rec, 'zip_code')                       AS zip_code,
    toDateTime64(JSONExtractInt(rec, 'created_at') / 1000000, 6)  AS created_at,
    toDateTime64(JSONExtractInt(rec, 'updated_at') / 1000000, 6)  AS updated_at,
    JSONExtractUInt(raw, 'ts_ms')                            AS ver,
    if(op = 'd', 1, 0)                                       AS is_deleted
FROM mirror.kafka_customers
WHERE length(raw) > 0 AND rec NOT IN ('', 'null');

CREATE VIEW IF NOT EXISTS mirror.customers_current AS
SELECT * EXCEPT (ver, is_deleted) FROM mirror.customers FINAL WHERE is_deleted = 0;

-- ── products ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mirror.products
(
    product_id     Int64,
    name           String,
    description    String,
    price          Float64,
    category       LowCardinality(String),
    stock_quantity Int32,
    created_at     DateTime64(6),
    updated_at     DateTime64(6),
    ver            UInt64,
    is_deleted     UInt8
)
ENGINE = ReplacingMergeTree(ver, is_deleted)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS mirror.kafka_products (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'cdc.public.products',
         kafka_group_name = 'clickhouse-mirror-products',
         kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS mirror.products_mv TO mirror.products AS
WITH
    JSONExtractString(raw, 'op') AS op,
    if(op = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS rec
SELECT
    JSONExtractInt(rec, 'product_id')                        AS product_id,
    JSONExtractString(rec, 'name')                           AS name,
    JSONExtractString(rec, 'description')                    AS description,
    JSONExtractFloat(rec, 'price')                           AS price,
    JSONExtractString(rec, 'category')                       AS category,
    toInt32(JSONExtractInt(rec, 'stock_quantity'))           AS stock_quantity,
    toDateTime64(JSONExtractInt(rec, 'created_at') / 1000000, 6)  AS created_at,
    toDateTime64(JSONExtractInt(rec, 'updated_at') / 1000000, 6)  AS updated_at,
    JSONExtractUInt(raw, 'ts_ms')                            AS ver,
    if(op = 'd', 1, 0)                                       AS is_deleted
FROM mirror.kafka_products
WHERE length(raw) > 0 AND rec NOT IN ('', 'null');

CREATE VIEW IF NOT EXISTS mirror.products_current AS
SELECT * EXCEPT (ver, is_deleted) FROM mirror.products FINAL WHERE is_deleted = 0;

-- ── order_items ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mirror.order_items
(
    item_id    Int64,
    order_id   Int64,
    product_id Int64,
    quantity   Int32,
    unit_price Float64,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    ver        UInt64,
    is_deleted UInt8
)
ENGINE = ReplacingMergeTree(ver, is_deleted)
ORDER BY item_id;

CREATE TABLE IF NOT EXISTS mirror.kafka_order_items (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'cdc.public.order_items',
         kafka_group_name = 'clickhouse-mirror-order-items',
         kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS mirror.order_items_mv TO mirror.order_items AS
WITH
    JSONExtractString(raw, 'op') AS op,
    if(op = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS rec
SELECT
    JSONExtractInt(rec, 'item_id')                           AS item_id,
    JSONExtractInt(rec, 'order_id')                          AS order_id,
    JSONExtractInt(rec, 'product_id')                        AS product_id,
    toInt32(JSONExtractInt(rec, 'quantity'))                 AS quantity,
    JSONExtractFloat(rec, 'unit_price')                      AS unit_price,
    toDateTime64(JSONExtractInt(rec, 'created_at') / 1000000, 6)  AS created_at,
    toDateTime64(JSONExtractInt(rec, 'updated_at') / 1000000, 6)  AS updated_at,
    JSONExtractUInt(raw, 'ts_ms')                            AS ver,
    if(op = 'd', 1, 0)                                       AS is_deleted
FROM mirror.kafka_order_items
WHERE length(raw) > 0 AND rec NOT IN ('', 'null');

CREATE VIEW IF NOT EXISTS mirror.order_items_current AS
SELECT * EXCEPT (ver, is_deleted) FROM mirror.order_items FINAL WHERE is_deleted = 0;
