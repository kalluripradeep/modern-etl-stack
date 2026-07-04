{{ config(
    materialized='view'
) }}

with int_orders as (
    select * from {{ ref('orders_clean') }}
),

int_order_items as (
    select * from {{ ref('order_items_clean') }}
)

select
    oi.item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    oi.quantity,
    oi.unit_price,
    o.total_amount as order_total_amount,
    o.status as order_status,
    o.created_at as order_created_at,
    o.updated_at as order_updated_at,
    (oi.quantity * oi.unit_price) as item_total_price,
    CURRENT_TIMESTAMP as dbt_updated_at
from int_order_items as oi
inner join int_orders as o on oi.order_id = o.order_id
