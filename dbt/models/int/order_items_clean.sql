{{ config(
    materialized='table',
    unique_key='item_id'
) }}

with raw_order_items as (
    select * from {{ source('raw', 'order_items_source') }}
)

select
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at,
    updated_at
from raw_order_items
where
    quantity > 0
    and unit_price >= 0
