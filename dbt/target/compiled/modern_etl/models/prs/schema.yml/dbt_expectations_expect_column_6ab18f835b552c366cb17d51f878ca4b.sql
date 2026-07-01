






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and total_orders >= 1
)
 as expression


    from "destdb"."prs"."v_daily_revenue"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







