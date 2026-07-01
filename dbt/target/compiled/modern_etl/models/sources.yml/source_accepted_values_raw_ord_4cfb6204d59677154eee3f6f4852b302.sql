
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "destdb"."raw"."orders"
    group by status

)

select *
from all_values
where value_field not in (
    'pending','processing','shipped','delivered','cancelled'
)


