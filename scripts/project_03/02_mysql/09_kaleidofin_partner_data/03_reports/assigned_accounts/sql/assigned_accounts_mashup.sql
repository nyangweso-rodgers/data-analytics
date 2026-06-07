WITH 
assigned_accounts_cte as (
	SELECT accountId FROM kaleidofin_partner_data.assigned_accounts
	),
accounts_cte as (
	SELECT id, accountRef, accountTypeId, status, customerId, jsfDate, fullDepositDate, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.accounts
	),
assassigned_accounts_mashup_cte as (
	select assigned_accounts_cte.accountId,
	accounts_cte.status
	from assigned_accounts_cte
	left join accounts_cte on accounts_cte.id = assigned_accounts_cte.accountId 
	)
select distinct status, count(*)
from assassigned_accounts_mashup_cte
GROUP by 1
order by 2 desc