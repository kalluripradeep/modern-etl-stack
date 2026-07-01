select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select email
from "destdb"."raw"."customers_source"
where email is null



      
    ) dbt_internal_test