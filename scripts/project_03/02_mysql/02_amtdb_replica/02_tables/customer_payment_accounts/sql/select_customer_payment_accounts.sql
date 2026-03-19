WITH 
customer_payment_accounts_cte as (
	SELECT id, customer_id, actual_balance, available_balance, refundable_balance, is_active, is_migrated, rule_id, created_at, 
	updated_at, created_by, updated_by, wallet_type_id, amt_account_id, account_type_id, days_overdue, installment_amount, is_overdue, next_payment_due_date, balances_calculated_at, last_ledger_activity_at, recalculation_in_progress, 
	version
	FROM amtdb.customer_payment_accounts
	)
select *
#count(*)
from customer_payment_accounts_cte
limit 100