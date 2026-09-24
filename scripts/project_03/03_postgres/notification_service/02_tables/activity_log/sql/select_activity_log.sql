with
activity_logs_cte as (
	SELECT id, activity, payload, "createdBy", "timestamp", app
	FROM public."ActivityLog"
	)
select *
from activity_logs_cte
limit 100