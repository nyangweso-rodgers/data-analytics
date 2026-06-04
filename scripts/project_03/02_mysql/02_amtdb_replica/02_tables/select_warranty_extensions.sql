with
warranty_extensions_cte as (
	SELECT id, 
	accountId, 
	startDate, 
	endDate, 
	warrantyPeriod, 
	isActive, 
	#productWarrantyConfigId, 
	createdAt, 
	updatedAt
	#createdBy, updatedBy
	FROM amtdb.warranty_extensions
	)
select *
from warranty_extensions_cte
limit 100