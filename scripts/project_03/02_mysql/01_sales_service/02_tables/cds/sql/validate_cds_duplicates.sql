with
cds_cte as (
	SELECT distinct id, 
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
validate_duplicate_records_cte as (
	select distinct id, cdsId, leadId, customerId, createdAt, 
	count(*) as recordId 
	from cds_cte
	GROUP BY 1,2,3,4,5
	HAVING recordId > 1
	ORDER BY recordId desc
	),
duplicete_records_cte as (
	select *
	from cds_cte
	where leadId in (select distinct leadId from validate_duplicate_records_cte)
	and accountId is null
	order by leadId 
	)
SELECT *
from validate_duplicate_records_cte
#from duplicete_records_cte