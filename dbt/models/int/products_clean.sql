{{ config(
    materialized='table',
    unique_key='product_id'
) }}

with raw_products as (
    select * from {{ source('raw', 'products_source') }}
)

select
    product_id,
    name,
    description,
    price,
    category,
    stock_quantity,
    created_at,
    updated_at
from raw_products
where price >= 0
