
  
    

  create  table "destdb"."analytics"."silver_orders__dbt_tmp"
  
  
    as
  
  (
    -- Silver Layer: Cleaned and validated orders
-- Removes duplicates, nulls, and invalid data



WITH cleaned_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        created_at,
        updated_at,
        -- Add data quality flags
        CASE 
            WHEN total_amount <= 0 THEN 'invalid_amount'
            WHEN status IS NULL THEN 'missing_status'
            WHEN order_date IS NULL THEN 'missing_date'
            ELSE 'valid'
        END as quality_flag
    FROM "destdb"."analytics"."bronze_orders"
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
WHERE quality_flag = 'valid'  -- Only keep valid records
    AND order_id IS NOT NULL   -- No null IDs
    AND total_amount > 0       -- Positive amounts only
  );
  