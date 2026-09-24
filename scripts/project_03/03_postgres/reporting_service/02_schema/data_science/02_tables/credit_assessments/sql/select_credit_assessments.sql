with
credit_assessments_cte as (
	SELECT assessment_id, customer_id, risk_score, risk_level, model_version, data_completeness, created_at, updated_at, feature_list, 
	batch_job_id, deterministic_fills_used, snapshot_month
	FROM data_science.credit_assessments
	)
select *
from credit_assessments_cte
where customer_id in ('54230')