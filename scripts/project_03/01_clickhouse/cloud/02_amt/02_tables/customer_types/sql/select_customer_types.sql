WITH
--------------------- Customer Types ----------------------------------
customer_types_cte as (
        select *
        from (
                SELECT id,
                customerType,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.customer_types
                ) where rnk = 1 
)
select *
from customer_types_cte