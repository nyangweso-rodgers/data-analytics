with
credit_checks_cte as (
	SELECT id, 
	isActive, 
	creditCheckId, 
	creditCheckPlatform, 
	customerId, 
	creditCheckDate, 
	salesForceCdsNumber, 
	salesForceCdsId, 
	cds_2_rm, 
	cds_1_completionDate, 
	cds_1_AgentId, 
	cds_2_completionDate, 
	score, 
	status, 
	cdsDate, 
	createdBy, 
	updatedBy, 
	createdAt, 
	updatedAt
	FROM amtdb.credit_checks
	)
select *
#distinct status, count(distinct id) as id_count
from credit_checks_cte
#where date(cds_2_completionDate) = '2025-04-22'
where customerId = '136016'
#group by 1
#order by 2 desc