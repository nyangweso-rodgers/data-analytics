with
account_payplans_cte as (
	SELECT id, accountId, payplanId, productQty, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.account_payplans
	)
select count(*)
from account_payplans_cte