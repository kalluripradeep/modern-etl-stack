{{ config(
    materialized='view'
) }}

select
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    address,
    city,
    state,
    zip_code,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as dbt_updated_at
from {{ ref('customers_clean') }}
