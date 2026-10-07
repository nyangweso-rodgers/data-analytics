with
ml_payg_default_model_predictions_cte as (
	SELECT id, accountId, accountRef, customerId, score_month, tier, tier_source, default_probability, recommendation, 
	recommendation_detail, model_version, feature_run_id, scoring_rules_version, totalArrearsAmount, totalOutstandingBalance, totalPaidAmount, 
	totalLoanAmount, outstanding_balance, daysLate, accountStatus, funnel_status, country, region, county, subcounty, town, 
	#latitude, longitude, 
	#customerName, customerPhoneNumber, customerIdentificationNumber, customer_age, gender, sale_date, jsfDate, expectedDate, last_payment_date, installment_number, next_expected_amount, months_overdue, consecutive_misses, 
	#loan_age_months, prior_payment_rate, recent_3m_payment_rate, days_since_last_payment, productName, productQty, 
	scored_at, createdAt, createdBy
	FROM reporting_service.ml_payg_default_model_predictions
	where score_month = '2026_08'
	#where date(scored_at) = '2026-08-02'
	and recommendation not in ('NO_ACTION_COMPLETE', 'NO_ACTION_REPOSSESSED', 'NO_ACTION_WRITEOFF', 'NO_ACTION_REPO_ON_HOLD') 
	and country = 'kenya'
	),
agg_ml_payg_default_model_predictions_cte as (
	select distinct score_month,
	scored_at, 
	#country,
	recommendation, 
	count(distinct accountId) as account_id_count,
	count(distinct customerId) as customer_id_count
	from ml_payg_default_model_predictions_cte
	group by 1,2,3
	order by 1, 4 desc
	)
select *
from agg_ml_payg_default_model_predictions_cte
limit 1000