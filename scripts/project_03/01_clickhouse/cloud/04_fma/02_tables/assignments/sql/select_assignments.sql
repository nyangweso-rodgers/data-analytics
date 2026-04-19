WITH
--------------------- assignments ----------------------------------
assignments_cte as (
    select *
    from (
        SELECT distinct id,
        assignment_type,
        account_id,
        ticket_id,
        ticket_number,
        number,
        is_active,
        premises_id,
        engineer_id,
        comment,
        created_at,
        assignment_date,  
        updated_at,  
        sync_at,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
    FROM fma.assignments
    ) where rnk = 1
    )
select *
from assignments_cte
limit 1000