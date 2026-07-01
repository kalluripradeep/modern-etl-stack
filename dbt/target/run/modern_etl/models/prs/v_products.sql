
  create view "destdb"."prs"."v_products__dbt_tmp"
    
    
  as (
    

SELECT
    product_id,
    name,
    description,
    price,
    category,
    stock_quantity,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM "destdb"."int"."int_products"
  );