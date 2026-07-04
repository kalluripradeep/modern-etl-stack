-- Minimal seed for the CI dbt integration test.
-- Mirrors the raw-layer DDL created by airflow/dags/ingest_source_to_bronze.py
-- and satisfies every test in dbt/models (sources.yml + schema.yml).

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers_source (
    customer_id  BIGINT PRIMARY KEY,
    first_name   TEXT,
    last_name    TEXT,
    email        TEXT UNIQUE,
    address      TEXT,
    city         TEXT,
    state        TEXT,
    zip_code     TEXT,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.products_source (
    product_id     BIGINT PRIMARY KEY,
    name           TEXT,
    description    TEXT,
    price          NUMERIC(18,2),
    category       TEXT,
    stock_quantity INTEGER,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.orders_source (
    order_id     BIGINT PRIMARY KEY,
    customer_id  BIGINT,
    order_date   TIMESTAMP,
    total_amount NUMERIC(18,2),
    status       TEXT,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.order_items_source (
    item_id    BIGINT PRIMARY KEY,
    order_id   BIGINT,
    product_id BIGINT,
    quantity   INTEGER,
    unit_price NUMERIC(18,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

INSERT INTO raw.customers_source VALUES
    (1, 'Ada',   'Lovelace', 'ada@example.com',   '1 Analytical Way', 'London',   'LDN', 'E1 6AN', now(), now()),
    (2, 'Grace', 'Hopper',   'grace@example.com', '2 Compiler Ct',    'New York', 'NY',  '10001',  now(), now()),
    (3, 'Alan',  'Turing',   'alan@example.com',  '3 Enigma Rd',      'Bletchley','BKM', 'MK3 6EB', now(), now())
ON CONFLICT DO NOTHING;

INSERT INTO raw.products_source VALUES
    (10, 'Keyboard', 'Mechanical keyboard', 89.99, 'electronics', 40, now(), now()),
    (20, 'Monitor',  '27-inch display',    249.00, 'electronics', 15, now(), now()),
    (30, 'Desk Mat', 'Large desk mat',      19.50, 'accessories', 80, now(), now())
ON CONFLICT DO NOTHING;

INSERT INTO raw.orders_source VALUES
    (100, 1, now() - interval '3 days', 109.49, 'delivered',  now(), now()),
    (101, 2, now() - interval '2 days', 249.00, 'shipped',    now(), now()),
    (102, 3, now() - interval '2 days',  89.99, 'processing', now(), now()),
    (103, 1, now() - interval '1 day',   19.50, 'pending',    now(), now()),
    (104, 2, now() - interval '1 day',  269.49, 'cancelled',  now(), now())
ON CONFLICT DO NOTHING;

INSERT INTO raw.order_items_source VALUES
    (1000, 100, 10, 1, 89.99, now(), now()),
    (1001, 100, 30, 1, 19.50, now(), now()),
    (1002, 101, 20, 1, 249.00, now(), now()),
    (1003, 102, 10, 1, 89.99, now(), now()),
    (1004, 103, 30, 1, 19.50, now(), now()),
    (1005, 104, 20, 1, 249.00, now(), now()),
    (1006, 104, 30, 1, 19.50, now(), now())
ON CONFLICT DO NOTHING;
