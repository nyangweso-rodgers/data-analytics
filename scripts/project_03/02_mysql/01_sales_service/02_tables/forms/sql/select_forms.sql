with
forms_cte as (
	SELECT id, name, description, formType, companyRegionId, status, 
	#archivedAt, 
	createdAt, updatedAt, 
	#slug, 
	createdBy, updatedBy, formTypeId, publishedAt, publishedBy
	#archivedBy, submissionConfig
	FROM `sales-service`.forms
	)
select *
from forms_cte