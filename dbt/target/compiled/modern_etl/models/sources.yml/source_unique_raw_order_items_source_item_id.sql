
    
    

select
    item_id as unique_field,
    count(*) as n_records

from "destdb"."raw"."order_items_source"
where item_id is not null
group by item_id
having count(*) > 1


