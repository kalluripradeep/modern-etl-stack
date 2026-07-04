{{ config(
    materialized='table',
    unique_key='order_id',
    tags=['int', 'orders']
) }}

with cleaned_orders as (
    select
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        created_at,
        updated_at,
        case
            when total_amount <= 0 then 'invalid_amount'
            when status is NULL then 'missing_status'
            when order_date is NULL then 'missing_date'
            else 'valid'
        end as quality_flag
    from {{ source('raw', 'orders_source') }}
)

select
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    created_at,
    updated_at
from cleaned_orders
where
    quality_flag = 'valid'
    and order_id is not NULL
    and total_amount > 0
