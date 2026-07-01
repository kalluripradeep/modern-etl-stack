
  create view "destdb"."prs"."v_customers__dbt_tmp"
    
    
  as (
    

SELECT
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
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM "destdb"."int"."int_customers"
  );