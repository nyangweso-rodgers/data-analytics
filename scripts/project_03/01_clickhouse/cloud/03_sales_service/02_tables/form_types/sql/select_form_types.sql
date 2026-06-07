WITH
form_types_cte as (
    select *
    from (
        SELECT id,
        name,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM `sales-service`.form_types
    ) where rnk =1
    )
select *
from form_types_cte
ORDER BY id
LIMIT 31 OFFSET 0;