select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select stock_quantity
from "destdb"."int"."products_clean"
where stock_quantity is null



      
    ) dbt_internal_test