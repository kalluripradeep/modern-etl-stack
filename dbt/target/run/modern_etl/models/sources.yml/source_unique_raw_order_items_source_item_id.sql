select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    item_id as unique_field,
    count(*) as n_records

from "destdb"."raw"."order_items_source"
where item_id is not null
group by item_id
having count(*) > 1



      
    ) dbt_internal_test