{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- Conformed product dimension. list_price is the current catalogue price;
-- the price actually charged lives on the fact as unit_price.

select
    product_id,
    name as product_name,
    category,
    price as list_price,
    stock_quantity,
    created_at,
    updated_at,
    current_timestamp as dbt_updated_at
from {{ ref('products_clean') }}
