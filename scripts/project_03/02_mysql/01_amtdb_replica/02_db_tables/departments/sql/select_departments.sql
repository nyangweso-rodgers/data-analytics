with
departments_cte as (
	SELECT id, 
	#old_id, companyRegionId, 
	name, 
	#hod, 	createdAt, updatedAt
	FROM amtdb.departments
	)
select distinct name
from departments_cte
#where id = '13'
order by name