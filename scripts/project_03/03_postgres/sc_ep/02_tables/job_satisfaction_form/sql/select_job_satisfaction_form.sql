with
jsf_cte as (
	SELECT --meta, 
	id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	jsf_status, 
	schedule_id, 
	jsf_type, 
	completed_date, 
	device_id, 
	device_status, 
	casual_pay, 
	costings, 
	outcome_reason, 
	jsf_start_time, 
	jsf_end_time, 
	engineer_recommendation, 
	device_image, 
	product_type, 
	"comment", 
	approval_date, 
	approved_by, 
	submission_date, 
	submitted_by
	FROM public.job_satisfaction_form
	)
select --*
count(*), count(distinct id), count(distinct schedule_id)
from jsf_cte