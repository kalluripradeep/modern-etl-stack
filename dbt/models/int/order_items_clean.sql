{{ config(
    materialized='table',
    unique_key='item_id'
) }}

WITH raw_order_items AS (
    SELECT * FROM {{ source('raw', 'order_items_source') }}
)

SELECT
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at,
    updated_at
FROM raw_order_items
WHERE quantity > 0
    AND unit_price >= 0
