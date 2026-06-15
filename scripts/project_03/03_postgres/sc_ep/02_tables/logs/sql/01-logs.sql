with
logs_cte as (
	SELECT id, 
	created_at, 
	updated_at,
	action_type, 
	--table_name, 
	obj_id, 
	"data", 
	(data::jsonb)::jsonb->>'schedule_id' AS schedule_id,
    (data::jsonb)::jsonb->>'jsf_type' AS jsf_type,
	created_by, 
	note
	FROM public.logs
	where table_name = 'job_satisfaction_form'
	)
select *
--distinct table_name, action_type
--distinct note
from logs_cte 
--and id = '1'
--order by 1,2 --updated_at desc
limit 100 