with
supervisors_cte as (
	SELECT id, 
	companyRegionId, 
	employeeId, 
	supervisorId, 
	createdAt, 
	createdBy, 
	updatedAt, 
	updatedBy
	FROM amtdb.supervisors
	)
select *
from supervisors_cte