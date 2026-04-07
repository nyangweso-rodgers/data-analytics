with
form_answers_cte as (
	SELECT id, leadId, questionId, cdsId, answer, 
	createdBy, updatedBy, createdAt, updatedAt, 
	answerSetId, 
	questionTemplateId, questionInstanceId, answerType, valuePayload, isNull, 
	recordedAt, 
	expectedAnswerId
	FROM `sales-service`.form_answers
	),
check_duplicates_cte as (
	select 
	#count(*),
	distinct cdsId, count(*) as id_count
	from form_answers_cte 
	where cdsId is not null
	GROUP by 1
	having id_count > 1
	order by 2 desc
	)
select *
#count(*), count(distinct cdsId) as cds_id_count
#distinct answer, count(distinct cdsId)
from form_answers_cte
#where questionId ='did_you_previously_own_a_water_pump_c'
where cdsId = '8141'
#where cdsId is null
#group by 1 order by 2 desc
#limit 100