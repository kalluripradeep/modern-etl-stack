{{ config(
    materialized='table',
    unique_key='item_id'
) }}

WITH silver_orders AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

silver_order_items AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
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
FROM silver_order_items oi
JOIN silver_orders o ON oi.order_id = o.order_id
