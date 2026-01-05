with
kyc_requests_cte as (
	SELECT id, externalRefId, leadId, idNumber, serialNumber, dob, status, 
	#description, 
	companyRegionId, 
	#meta, createdBy, updatedBy, 
	createdAt, updatedAt, documentType_temp, documentType, 
	#callbackJsonBlob, 
	smileJobId, resultCode, resultText, actions, source, gender, 
	#description2, isActive, formId, formVersion, deletedAt, deletedBy, archivedAt, archivedBy, lastModifiedAt, lastModifiedBy, 
	is_migrated
	FROM `sales-service`.kyc_requests
	)
select #*
#externalRefId, 
count(*)
#distinct dob
from kyc_requests_cte
#where externalRefId is null
#where externalRefId = '01KC4GMDQ6EC0GX6B22PYSMW82-1765383280513'
#group by 1
#order by 2 desc
limit 1000