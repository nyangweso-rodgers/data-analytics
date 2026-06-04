with
commission_packages_cte as (
	SELECT id, 
	startDate, 
	endDate, 
	payplanId, 
	eligibilityId, 
	totalUnlockableCommission, 
	amount, 
	payPlanAmount, 
	depositAmount, 
	installmentAmount, 
	createdBy, 
	updatedBy, 
	createdAt, 
	updatedAt
	FROM amtdb.commission_packages
	)
select *
from commission_packages_cte