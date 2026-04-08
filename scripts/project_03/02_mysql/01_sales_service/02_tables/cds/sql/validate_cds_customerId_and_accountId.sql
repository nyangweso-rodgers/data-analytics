with
cds_cte as (
	SELECT distinct #id, 
	cdsId, 
	leadId, 
	cds1CompletionDate, 
	#cds1Tracker, 
	cds2CompletionDate, 
	#cdsSource, 
	#email, # all NULL
	#mobileNumber, # all NULL
	#productId, # all NULL
	creditCheckStatus, 
	#creditScore, creditScoreDate, 
	stage, 
	#surveyCompleted, 
	#nextOfKinEmail, # all NULL
	#nextOfKinGender, # all NULL
	#lastModifiedById, createdBy, updatedBy, 
	createdAt, 
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
leads_cte as (
	select leadId,
	companyRegionId 
	from `sales-service`.leads
	),
cds_leads_mashup_cte as (
	select cds_cte.*,
	leads_cte.companyRegionId 
	from cds_cte
	left join leads_cte on leads_cte.leadId = cds_cte.leadId 
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
validate_cds_accountId_cte as (
	select distinct 
	#cdsId,
	leadId,
	customerId,
	accountId,
	creditCheckStatus,
	cds1CompletionDate,
	cds2CompletionDate,
	is_migrated,
	createdAt,
	updatedAt,
	companyRegionId
	from cds_leads_mashup_cte
	where (customerId is not null)
	and (accountId is null)
	#and creditCheckStatus is not null
	)
SELECT *
#count(*), count(distinct leadId), count(distinct customerId)
#distinct cdsSource
#distinct leadId, createdAt, customerId, accountId  
#from cds_cte
#from validate_customerId_cte
from validate_cds_accountId_cte
#where leadId = ''
#where accountId in ('')
#where customerId = ''
#limit 100
ORDER BY leadId, createdAt desc