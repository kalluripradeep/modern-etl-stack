
  
    

  create  table "destdb"."int"."int_order_items__dbt_tmp"
  
  
    as
  
  (
    

WITH raw_order_items AS (
    SELECT * FROM "destdb"."raw"."order_items"
)

SELECT
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at,
    updated_at
FROM raw_order_items
WHERE quantity > 0
    AND unit_price >= 0
  );
  