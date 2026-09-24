with
agg_credit_history_v1_cte as (
	SELECT country, customer_id, customer_wallet_is_migrated, customer_wallet_id, account_id, account_ref, account_type, account_status, deposit_amount, installment_amount, total_payplan_amount, total_number_payments, payment_sequence, installment_type, expected_date, expected_amount, final_amount_paid, final_paid_date, amount_due, days_late, is_fully_paid, is_due, overdue_balance, total_balance, future_balance, sync_timestamp
	FROM data_science.agg_credit_history_v1
	)
select *
from agg_credit_history_v1_cte
where customer_id in ('54230')