with
employee_regions_cte as (
	SELECT id, companyRegionId, employeeId
	#createdAt, updatedAt
	FROM amtdb.employee_regions
	)
select *
from employee_regions_cte