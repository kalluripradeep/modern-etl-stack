{{ config(
    materialized='view'
) }}

WITH int_orders AS (
    SELECT * FROM {{ ref('orders_clean') }}
),

int_order_items AS (
    SELECT * FROM {{ ref('order_items_clean') }}
)

SELECT
    oi.item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) AS item_total_price,
    o.total_amount AS order_total_amount,
    o.status AS order_status,
    o.created_at AS order_created_at,
    o.updated_at AS order_updated_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM int_order_items oi
JOIN int_orders o ON oi.order_id = o.order_id
