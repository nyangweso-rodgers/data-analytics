with
account_types_cte as (
	SELECT id, accountType, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.account_types
	)
select COUNT(*)
from account_types_cte