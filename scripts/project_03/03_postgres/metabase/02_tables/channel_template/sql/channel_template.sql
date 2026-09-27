with
channel_template_cte as (
	SELECT id, "name", channel_type, details, created_at, updated_at
	FROM public.channel_template
	)
select *
from channel_template_cte