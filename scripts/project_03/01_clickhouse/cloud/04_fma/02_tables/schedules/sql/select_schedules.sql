WITH
--------------------- schedules ----------------------------------
schedules_cte as (
    select *
    from (
        SELECT distinct id,
        is_active,
        assignment_id,
        created_at, 
        scheduled_date,
        completed_date,
        updated_at,  
        sync_at,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
    FROM fma.schedules
    ) where rnk = 1
    )
select *
from schedules_cte
limit 1000