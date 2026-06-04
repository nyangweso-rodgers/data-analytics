WITH
commission_milestones_cte as (
	SELECT id, 
	commissionPackageId, 
	incentiveId, 
	payCircle, 
	paymentDay, 
	description, 
	startDate, 
	endDate, 
	accountTypeId, 
	milestone, 
	amount, 
	percentage, 
	narrative, 
	createdBy, 
	updatedBy, 
	createdAt, 
	updatedAt
	FROM amtdb.commission_milestones
	)
select *
from commission_milestones_cte