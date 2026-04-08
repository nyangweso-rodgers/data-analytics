with
cds_cte as (
	SELECT id, cdsId, leadId, 
	cds1CompletionDate, 
	#cds1Tracker, 
	cds2CompletionDate, 
	#cdsSource, 
	#email, # all NULL
	#mobileNumber, # all NULL
	#productId, # all NULL
	creditCheckStatus, creditScore, creditScoreDate, stage, 
	#surveyCompleted, 
	#nextOfKinEmail, # all NULL
	#nextOfKinGender, # all NULL
	#lastModifiedById, createdBy, updatedBy, 
	#createdAt, 
	updatedAt, 
	#isActive, 
	#formId, # all NULL 
	#formVersion, # all NULL 
	#deletedAt, deletedBy, archivedAt, archivedBy, lastModifiedAt, lastModifiedBy, 
	is_migrated,
	#formVersionId # all NULL
	customerId,
	accountId
	#cds1CompletionBy, cds2CompletionBy, creditCheckCompletedBy
	FROM `sales-service`.cds                 
	),
validate_customerId_cte as (
	select distinct leadId,
	customerId,
	accountId,
	creditCheckStatus,
	cds1CompletionDate,
	cds2CompletionDate,
	is_migrated,
	updatedAt
	from cds_cte
	where customerId is null
	),
validate_customer_accountId_cte as (
	select distinct leadId,
	customerId,
	accountId,
	creditCheckStatus,
	cds1CompletionDate,
	cds2CompletionDate,
	is_migrated,
	updatedAt
	from cds_cte
	where (customerId is not null)
	and (accountId is null)
	#and creditCheckStatus is not null
	)
SELECT #*
count(*), count(distinct leadId)
#distinct cdsSource
#from cds_cte
from validate_customerId_cte
#from validate_customer_accountId_cte
#where customerId in ('10296')
#limit 100