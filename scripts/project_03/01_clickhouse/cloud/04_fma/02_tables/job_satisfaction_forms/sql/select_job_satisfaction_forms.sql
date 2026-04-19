WITH
--------------------- JSF ----------------------------------
job_satisfaction_forms_cte as (
    select *
    from (
        SELECT distinct id,
        schedule_id,
        jsf_type,
        jsf_status,
        is_active,
        product_type,
        engineer_recommendation,
        outcome_reason,
        comment,
        jsf_start_time,
        jsf_end_time,
        completed_date,
        approval_date,
        submission_date,
        created_at,  
        updated_at,  
        sync_at,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
    FROM fma.job_satisfaction_forms
    ) where rnk = 1
    )
select *
from job_satisfaction_forms_cte
--where id in ()
limit 1000