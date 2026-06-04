with
ledger_entries_cte as (
	SELECT id, amount, previous_balance, new_balance, debit, credit, reference, 
	#metadata, 
	status, is_reversal, created_at, updated_at, 
	#created_by, updated_by, 
	account_id, 
	#original_entry_id, 
	transaction_type_id, transaction_timestamp
	FROM amtdb.ledger_entries
	)
SELECT *
#count(*)
from ledger_entries_cte
#where account_id = '1022'
#where account_id = '28755'
#where account_id = '115539'
order by created_at asc
limit 1000