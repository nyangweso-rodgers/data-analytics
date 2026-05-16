with
assignments_cte as (
	SELECT --meta, 
	id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	premises_id, 
	engineer_id, 
	assignment_type, 
	assigned_by, 
	assignment_date, 
	account_id, 
	ticket_id, 
	ticket_number, 
	"number", 
	"comment"
	FROM public.assignments
	)
select 
--count(*), count(distinct id)
distinct assignment_type 
from assignments_cte