with
account_payplans_cte as (
	SELECT distinct id, 
	accountId, 
	payplanId, 
	productQty 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy
	FROM amtdb.account_payplans
	),
monthly_account_payplans_cte as (
	select distinct DATE_FORMAT(date(createdAt), '%Y-%m-01') as created_at_month,
	count(distinct id) as account_payplan_id_count
	from account_payplans_cte
	group by 1
	order by 1
	)	
SELECT sum(account_payplan_id_count) 
#from monthly_account_payplans_cte
from monthly_account_payplans_cte
where created_at_month <= '2025-05-01'
#where DATE_FORMAT(createdAt, '%Y-%m-01') = '2000-01-01'
#where id = '57181'