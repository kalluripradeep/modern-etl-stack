
  create view "destdb"."analytics"."bronze_orders__dbt_tmp"
    
    
  as (
    -- Bronze Layer: Raw data from MinIO/Parquet
-- This is a view that references the source table



SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    created_at,
    updated_at
FROM "destdb"."public"."orders"
  );