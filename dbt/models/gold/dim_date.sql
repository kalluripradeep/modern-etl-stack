{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- Calendar dimension spanning the observed order history. Built from the
-- fact's own date range rather than a fixed window, so it never carries
-- rows the star cannot reach. An empty orders table yields an empty spine
-- (generate_series over nulls returns no rows) rather than an error.

with bounds as (
    select
        min(order_date)::date as min_date,
        max(order_date)::date as max_date
    from {{ ref('orders_clean') }}
),

spine as (
    select
        generate_series(
            (select min_date from bounds),
            (select max_date from bounds),
            interval '1 day'
        )::date as date_day
)

select
    date_day,
    extract(year from date_day)::int as calendar_year,
    extract(quarter from date_day)::int as calendar_quarter,
    extract(month from date_day)::int as calendar_month,
    trim(to_char(date_day, 'Month')) as month_name,
    extract(day from date_day)::int as day_of_month,
    extract(isodow from date_day)::int as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    extract(isodow from date_day) >= 6 as is_weekend,
    date_trunc('month', date_day)::date as month_start_date,
    current_timestamp as dbt_updated_at
from spine
