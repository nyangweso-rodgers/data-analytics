with
currencies_cte as (
	SELECT id, 
	companyRegionId, 
	name, 
	#abbreviation, 
	symbol, 
	#createdAt, 
	#updatedAt, 
	netSuiteCurrencyId
	FROM amtdb.currencies
	)
select * from currencies_cte