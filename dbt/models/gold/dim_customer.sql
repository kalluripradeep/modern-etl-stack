{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- Conformed customer dimension. Natural key (customer_id) is used as the
-- dimension key: the source is a system of record with stable ids, so a
-- surrogate key would add a join without adding meaning.

select
    customer_id,
    full_name,
    first_name,
    last_name,
    email,
    city,
    state,
    zip_code,
    created_at,
    updated_at,
    current_timestamp as dbt_updated_at
from {{ ref('customers_clean') }}
