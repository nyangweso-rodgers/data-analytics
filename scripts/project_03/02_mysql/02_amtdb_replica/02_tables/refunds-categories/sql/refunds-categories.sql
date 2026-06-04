with
refunds_categories_cte as (
	SELECT id, 
	name
	#description, 
	#isActive, 
	#createdAt, 
	#updatedAt, 
	#createdBy, 
	#updatedBy
	FROM amtdb.refund_categories
	)
select * from refunds_categories_cte