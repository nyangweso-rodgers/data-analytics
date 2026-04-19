with
installment_payments_history_cte as (
	SELECT id, accountId, instalmentScheduleId, paymentId, paymentType, amtPaid, amtRefund, paidDate, updatedAt, updatedBy, createdAt, createdBy
	FROM amtdb.installment_payments_history
	)
select 
count(*)
from installment_payments_history_cte
where accountId is null