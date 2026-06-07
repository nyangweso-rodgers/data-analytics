with
premises_cte as (
	SELECT id, created_at, updated_at, account_id, premise_name, customer_id, substate_id, town, _exported_at
	FROM kaleidofin_partner_data.premises
	)
select count(*)
from premises_cte