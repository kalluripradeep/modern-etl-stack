select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

with all_values as (

    select
        status as value_field

    from "destdb"."int"."orders_clean"
    

),
set_values as (

    select
        cast('pending' as TEXT) as value_field
    union all
    select
        cast('processing' as TEXT) as value_field
    union all
    select
        cast('shipped' as TEXT) as value_field
    union all
    select
        cast('delivered' as TEXT) as value_field
    union all
    select
        cast('cancelled' as TEXT) as value_field
    
    
),
validation_errors as (
    -- values from the model that are not in the set
    select
        v.value_field
    from
        all_values v
        left join
        set_values s on v.value_field = s.value_field
    where
        s.value_field is null

)

select *
from validation_errors


      
    ) dbt_internal_test