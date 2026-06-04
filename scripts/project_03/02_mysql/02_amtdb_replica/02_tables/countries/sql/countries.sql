with
countries_cte as (
	SELECT id, 
	#old_id, 
	isoCode, 
	name, 
	createdAt, 
	updatedAt
	FROM amtdb.countries
	)
select * from countries_cte