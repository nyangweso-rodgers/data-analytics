WITH
--------------------- automations - collection_officer_assignments  ----------------------------------
collection_officer_assignments_cte as (
    SELECT * 
    FROM automations.collection_officer_assignments
    where companyRegion = 'kenya'
    and status in ('Current', 'Advance')
    ),
--------------------- agg - collection_officer_assignments  ---------------------------------- 
agg_collection_officer_assignments_cte as (
    select distinct status,
    assigned_function,
    assigned_employee_name,
    count(*) as record_count,
	count(distinct accountId) as account_id_count
    from collection_officer_assignments_cte
    group BY 1,2,3
    order by 1,2,3,5 desc
    )
select *
--from collection_officer_assignments_cte
from agg_collection_officer_assignments_cte
limit 1000