with
commission_eligibility_cte as (
	SELECT id, 
	startDate, 
	endDate, 
	payplanId, 
	incentiveId, 
	isActive, 
	createdBy, 
	updatedBy, 
	createdAt, 
	updatedAt
	FROM amtdb.commission_eligibility
	)
select * 
from commission_eligibility_cte
order by id