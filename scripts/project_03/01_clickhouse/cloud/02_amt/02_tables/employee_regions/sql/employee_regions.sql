WITH
--------------------- amt - employee_regions ----------------------------------
employee_regions_cte as (
    select *
    from (
        SELECT id,
    companyRegionId,
    employeeId,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
    FROM amt.employee_regions
    ) WHERE rnk = 1
    )
SELECT *
from employee_regions_cte
limit 100