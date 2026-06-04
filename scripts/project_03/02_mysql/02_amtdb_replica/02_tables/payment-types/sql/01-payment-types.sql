with
payment_types_cte as (
	SELECT id, 
	#old_id, 
	paymentType, 
	companyRegions 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy
	FROM amtdb.payment_types
	)
select distinct paymentType
from payment_types_cte