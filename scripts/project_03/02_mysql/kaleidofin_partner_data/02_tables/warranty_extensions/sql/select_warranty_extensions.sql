with
warranty_extensions_cte as (
	SELECT id, accountId, startDate, endDate, warrantyPeriod, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.warranty_extensions
	)
select 
count(*)
from warranty_extensions_cte
limit 10