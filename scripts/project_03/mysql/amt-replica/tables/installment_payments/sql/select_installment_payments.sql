with
installment_payments_cte as (
	SELECT id, 
	accountId, 
	#old_account_id, 
	instalmentScheduleId, 
	#old_instalment_schedule_id, 
	paymentId, 
	#old_payment_id, 
	paymentType, 
	amtPaid, 
	amtRefund, 
	paidDate, 
	date(paidDate) as paid_date,
	createdAt, 
	date(createdAt) as created_at,
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	#ledgerEntryID, 
	discountRefunds
	FROM amtdb.installment_payments
	)
select #*
count(distinct accountId) 
from installment_payments_cte