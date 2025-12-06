with
account_logs_cte as (
	SELECT id, 
	eventType, 
	payload, 
	accountId, 
	#old_account_id, 
	amount, 
	#note, 
	createdAt 
	#createdBy, 
	#old_created_by, 
	#updatedAt
	FROM amtdb.account_logs
	),
account_logs_agg_cte as (
	select accountId,
	COUNT(DISTINCT CASE WHEN eventType = 'change_product' THEN id END) AS change_product_count,
	#JSON_ARRAYAGG(DISTINCT CASE WHEN eventType = 'change_product' THEN DATE(createdAt) END) AS change_product_dates
	JSON_ARRAYAGG(DATE(createdAt)) FILTER (WHERE eventType = 'change_product') AS change_product_dates
	#COUNT(DISTINCT CASE WHEN eventType = 'change_payplan' THEN id END) AS change_payplan_count
	from account_logs_cte
	group by accountId
	)
select *
#distinct eventType, count(distinct id) as id_count
#from account_logs_cte
from account_logs_agg_cte
where accountId = '70821'
#order by createdAt