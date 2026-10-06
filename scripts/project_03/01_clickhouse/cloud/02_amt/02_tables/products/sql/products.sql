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
select distinct product
from products_cte
ORDER BY 1
limit 1000