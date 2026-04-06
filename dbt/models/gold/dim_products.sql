{{ config(
    materialized='table',
    unique_key='product_id'
) }}

WITH raw_products AS (
    SELECT * FROM {{ source('raw', 'products') }}
)

SELECT
    product_id,
    name,
    description,
    price,
    category,
    stock_quantity,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_products
WHERE price >= 0
