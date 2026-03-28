with
account_type_cte as (
	SELECT id, 
	#old_id, 
	accountType 
	#createdAt, createdBy, updatedAt, updatedBy
	FROM amtdb.account_types
	)
select distinct accountType
from account_type_cte