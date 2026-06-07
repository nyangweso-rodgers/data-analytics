with
sms_templates_cte as (
	SELECT id, "name", "text", params, "createdAt", "updatedAt", channel
	FROM public.sms_templates
	)
select --*
distinct name, count(distinct id)
from sms_templates_cte
group by 1 order by 2 desc
limit 1000