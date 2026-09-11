with
collection_officers_cte as (
	SELECT employee_id, employee_name, active, primary_role, created_at, created_by, updated_at, updated_by
	FROM reporting_service.collection_officers
	order by primary_role, employee_name
	)
SELECT *
from collection_officers_cte 