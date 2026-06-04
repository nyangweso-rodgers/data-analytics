with
view_types_cte as (
	SELECT id, 
	name
	#createdBy, createdAt, updatedBy, updatedAt
	FROM amtdb.view_types
	order by name
	)
select *
from view_types_cte
order by 1