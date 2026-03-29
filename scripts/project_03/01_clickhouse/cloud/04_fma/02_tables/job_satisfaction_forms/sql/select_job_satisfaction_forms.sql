WITH
job_satisfaction_forms_cte as (
    select *
    from (
        SELECT *,
    row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
    FROM fma.job_satisfaction_forms
    ) where rnk = 1
    )
select *
from job_satisfaction_forms_cte
where id in ()
limit 1000