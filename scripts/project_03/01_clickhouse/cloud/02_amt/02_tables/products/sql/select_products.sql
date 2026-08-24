WITH
--------------------- Products ----------------------------------
products_cte as (
    select *
    from (
            SELECT *,
            row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
            FROM amt.products
            ) where rnk = 1
    )
select *
from products_cte
limit 1000