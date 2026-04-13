with
ticket_priorities_cte as (
	SELECT id, "name", default_create, ui_icon, ui_color, note, active, updated_by_id, created_by_id, created_at, updated_at
	FROM public.ticket_priorities
	)
select 
--count(*)
max(updated_at)
from ticket_priorities_cte