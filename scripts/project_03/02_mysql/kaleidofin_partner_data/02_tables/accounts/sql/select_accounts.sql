with
accounts_cte as (
	SELECT id, accountRef, accountTypeId, status, customerId, jsfDate, fullDepositDate, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.accounts
	)
select count(*)
from accounts_cte