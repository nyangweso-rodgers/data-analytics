with
incentive_cte as (
	SELECT id, 
	name, 
	description, 
	isActive
	#createdBy, 
	#updatedBy, 
	#createdAt, 
	#updatedAt
	FROM amtdb.incentive
	)
select *
from incentive_cte
where id in (1,2)
