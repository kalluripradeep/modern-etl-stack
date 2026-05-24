{{ config(
    materialized='table',
    unique_key='order_id',
    tags=['int', 'orders']
) }}

WITH cleaned_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        created_at,
        updated_at,
        CASE 
            WHEN total_amount <= 0 THEN 'invalid_amount'
            WHEN status IS NULL THEN 'missing_status'
            WHEN order_date IS NULL THEN 'missing_date'
            ELSE 'valid'
        END as quality_flag
    FROM {{ source('raw', 'orders_source') }}
)

SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    created_at,
    updated_at
FROM cleaned_orders
WHERE quality_flag = 'valid'
    AND order_id IS NOT NULL
    AND total_amount > 0
