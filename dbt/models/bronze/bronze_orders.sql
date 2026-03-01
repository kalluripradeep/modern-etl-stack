-- Bronze Layer: Raw data from MinIO/Parquet
-- This is a view that references the source table

{{ config(
    materialized='view',
    tags=['bronze', 'orders']
) }}

SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    created_at,
    updated_at
FROM {{ source('raw', 'orders') }}