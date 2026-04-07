with
form_answer_sets_cte as (
	SELECT 
	id, formId, formVersion, respondentType, respondentId, referenceTable, referenceKey, submittedBy, submittedAt, contextJson, 
	createdAt, updatedAt, 
	referenceKeyColumn
	FROM `sales-service`.form_answer_sets
	)
select *
#count(*)
from form_answer_sets_cte
where id = '60637'
limit 1000