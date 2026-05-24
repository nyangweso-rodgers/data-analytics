with
agg_monthly_payments_v1_cte as (
	SELECT customer_created_date, 
	customer_id, 
	country, 
	customer_type, 
	payment_month, 
	total_payment_count, 
	total_payment_amount, 
	sync_timestamp
	FROM data_science.agg_monthly_payments_v1
	)
select 
count(*) as record_count, count(distinct customer_id) as customer_id_count, max(customer_created_date) as max_customer_created_date, max(payment_month) as max_payment_month, max(sync_timestamp)
from agg_monthly_payments_v1_cte