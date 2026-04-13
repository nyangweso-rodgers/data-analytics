with
ticket_states_cte as (
	SELECT id, state_type_id, "name", next_state_id, ignore_escalation, default_create, default_follow_up, note, active, updated_by_id, created_by_id, created_at, updated_at
	FROM public.ticket_states
	)
select 
count(*)
--max(updated_at)
from ticket_states_cte