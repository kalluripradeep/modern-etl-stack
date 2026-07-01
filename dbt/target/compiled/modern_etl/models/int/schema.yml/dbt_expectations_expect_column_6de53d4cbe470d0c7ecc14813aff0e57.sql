






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and price >= 0
)
 as expression


    from "destdb"."int"."products_clean"
    

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







