{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

WITH raw_customers AS (
    SELECT * FROM {{ source('raw', 'customers') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,
    email,
    address,
    city,
    state,
    zip_code,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_customers
