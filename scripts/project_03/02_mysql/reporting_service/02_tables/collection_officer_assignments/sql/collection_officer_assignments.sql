with
collection_officer_assignments_cte as (
	SELECT accountId, accountRef, accountType, status, funnel_status, product, 
	days_late_current, arrears, PowerBI_balance, 
	companyRegion, #latitude, longitude, town, County, region, 
	snapshot_date, 
	#first_expected_date, is_fpd_amount, fpd_amount_days_late, next_expected_date, next_payment_sequence, current_expected_date, 
	id, batch_id, 
	assigned_function, assigned_employee_id, assigned_employee_name, 
	assignment_start, assignment_end, created_at, 
	#created_by, 
	updated_at, 
	#updated_by, 
	note, active
	FROM reporting_service.collection_officer_assignments
	#where assigned_function not in ('Unassigned')
	),
agg_collection_officer_assignments_cte as (
	select distinct 
	assignment_start,
	assignment_end,
	snapshot_date,
	#status,
	count(*) as record_count
	from collection_officer_assignments_cte
	GROUP BY 1,2,3
	ORDER BY 1,2,3
	)	
select *
from collection_officer_assignments_cte
#from agg_collection_officer_assignments_cte
#WHERE id IN (133225, 111705);
where accountRef in ('39310286', '22838383')
order by accountRef
limit 1000