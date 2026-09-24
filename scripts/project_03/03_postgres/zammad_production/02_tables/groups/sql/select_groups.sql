with
groups_cte as (
	SELECT id, signature_id, email_address_id, "name", name_last, parent_id, 
	assignment_timeout, follow_up_possible, reopen_time_in_days, follow_up_assignment, active, shared_drafts, note, updated_by_id, created_by_id, created_at, updated_at
	FROM public."groups"
	)
select --*
count(*)
--max(updated_at)
from groups_cte