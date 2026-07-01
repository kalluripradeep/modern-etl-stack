
  
    

  create  table "destdb"."int"."int_products__dbt_tmp"
  
  
    as
  
  (
    

WITH raw_products AS (
    SELECT * FROM "destdb"."raw"."products"
)

SELECT
    product_id,
    name,
    description,
    price,
    category,
    stock_quantity,
    created_at,
    updated_at
FROM raw_products
WHERE price >= 0
  );
  