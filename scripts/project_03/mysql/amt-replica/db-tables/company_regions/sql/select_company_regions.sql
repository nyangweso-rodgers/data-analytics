with
company_regions_cte as (
	SELECT id, 
	region, 
	companyName 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	#defaultCurrencyId
	FROM amtdb.company_regions
	)
select *
from company_regions_cte