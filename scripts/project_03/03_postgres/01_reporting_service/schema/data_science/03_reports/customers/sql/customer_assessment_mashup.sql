with
test_accounts_features_v1_cte as (
	SELECT --customer_id, 
	cast(customer_id as varchar) as customer_id,
	account_id, 
	status,
	account_type,
	company_regions_id 
	--_feature_generated_at, 
	--_exported_at
	FROM data_science.test_accounts_features_v1
	),
credit_assessments_cte as (
	SELECT assessment_id, 
	customer_id,
	--CAST(customer_id AS INTEGER) as customer_id, 
	risk_score, 
	risk_level, 
	model_version, 
	data_completeness, 
	created_at, 
	updated_at, 
	feature_list, 
	batch_job_id, 
	deterministic_fills_used, 
	snapshot_month
	FROM data_science.credit_assessments
	),
mashup_cte as (
	select test_accounts_features_v1_cte.*,
	assessment_id,
	risk_score,
	risk_level,
	data_completeness, 
	snapshot_month, 
	created_at as assessment_created_at,
	updated_at as assessment_updated_at
	from test_accounts_features_v1_cte
	left join credit_assessments_cte on credit_assessments_cte.customer_id = test_accounts_features_v1_cte.customer_id
	where company_regions_id = 1 
	and account_type = 'PAYG'
	order by test_accounts_features_v1_cte.customer_id
	),
validate_customer_score_frequencies_cte as (
	select distinct customer_id,
	count(distinct assessment_id) as assessment_id_count
	from mashup_cte
	group by 1
	order by 2 desc
	)
select *
--count(*), count(distinct customer_id)
from mashup_cte
--from validate_customer_score_frequencies_cte
--where assessment_id is not null
where customer_id = '10001'