{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

-- Central fact of the star. Grain: one row per order line item.
--
-- Line grain (rather than order grain) keeps the additive measures honest:
-- line_amount sums correctly at every level, and order totals are recovered
-- with sum(line_amount) grouped by order_id. order_id is kept as a
-- degenerate dimension so order-level analysis needs no extra table.
--
-- The join to orders is an inner join on purpose: order_items_clean and
-- orders_clean have already dropped rows failing quality checks, and a line
-- whose order was rejected must not enter the fact.

with items as (
    select * from {{ ref('order_items_clean') }}
),

orders as (
    select * from {{ ref('orders_clean') }}
)

select
    -- degenerate dimensions
    i.item_id,
    i.order_id,

    -- foreign keys into the conformed dimensions
    o.customer_id,
    i.product_id,
    o.order_date::date as order_date_day,

    -- descriptive attributes
    o.status as order_status,
    o.order_date,

    -- additive measures
    i.quantity,
    i.unit_price,
    (i.quantity * i.unit_price) as line_amount,

    current_timestamp as dbt_updated_at
from items as i
inner join orders as o on i.order_id = o.order_id
