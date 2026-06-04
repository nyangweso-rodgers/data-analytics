with
payplans_cte as (
	SELECT id, productId, name, depositAmount, installmentAmount, totalNumberPayments, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.payplans
	)
select count(*)
from payplans_cte