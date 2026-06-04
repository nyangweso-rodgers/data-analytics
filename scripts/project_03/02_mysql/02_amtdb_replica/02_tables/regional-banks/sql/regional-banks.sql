with
regional_banks_cte as (
	SELECT id, 
	bankName, 
	companyRegionId, 
	code, 
	isActive, 
	createdAt, 
	updatedAt, 
	createdBy, 
	updatedBy, 
	currencyId, 
	paymentTypeId
	FROM amtdb.regional_banks
	)
select * from regional_banks_cte