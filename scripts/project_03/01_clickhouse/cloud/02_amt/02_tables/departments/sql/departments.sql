WITH
--------------------- amt - departments ----------------------------------
departments_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        FROM amt.departments
        ) where rnk = 1
    ) 
select *
from departments_cte 
LIMIT 31 OFFSET 0;