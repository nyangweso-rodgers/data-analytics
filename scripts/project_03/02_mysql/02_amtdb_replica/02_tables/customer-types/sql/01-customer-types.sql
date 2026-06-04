with
customer_types_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	saleType, 
	customerType 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy
	FROM amtdb.customer_types
	)