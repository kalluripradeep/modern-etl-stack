{{ config(
    materialized='view'
) }}

select
    status,
    DATE(order_date) as order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(total_amount) as min_order_value,
    MAX(total_amount) as max_order_value,
    CURRENT_TIMESTAMP as calculated_at
from {{ ref('orders_clean') }}
group by DATE(order_date), status
order by order_date desc, status asc
