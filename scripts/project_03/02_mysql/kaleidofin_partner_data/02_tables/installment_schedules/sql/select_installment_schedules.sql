with
installment_schedules_cte as (
	SELECT id, accountId, customerId, installmentType, paymentSequence, expectedAmount, expectedDate, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.installment_schedules
	)
select count(*)
from installment_schedules_cte 