{{ config(
    materialized='view'
) }}

select
    product_id,
    name,
    description,
    price,
    category,
    stock_quantity,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as dbt_updated_at
from {{ ref('products_clean') }}
