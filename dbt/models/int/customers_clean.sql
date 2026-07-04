{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

with raw_customers as (
    select * from {{ source('raw', 'customers_source') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    address,
    city,
    state,
    zip_code,
    created_at,
    updated_at,
    first_name || ' ' || last_name as full_name
from raw_customers
