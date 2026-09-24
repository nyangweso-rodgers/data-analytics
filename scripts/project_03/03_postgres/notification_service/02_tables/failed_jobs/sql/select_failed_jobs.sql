with 
failed_jobs_cte as (
	SELECT id, "type", "jobDetails", status, "createdAt", "updatedAt"
	FROM public."FailedJobs"
	)
select *
from failed_jobs_cte
limit 10