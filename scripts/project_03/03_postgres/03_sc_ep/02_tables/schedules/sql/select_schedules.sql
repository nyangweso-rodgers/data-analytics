with
schedules_cte as (
	SELECT meta, 
	id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	assignment_id, 
	scheduled_by, 
	scheduled_date, 
	completed_date
	FROM public.schedules
)
select --*
count(*), count(distinct id), count(distinct assignment_id)
from schedules_cte