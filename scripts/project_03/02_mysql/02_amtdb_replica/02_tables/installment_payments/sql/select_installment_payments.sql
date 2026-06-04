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
	createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	#ledgerEntryID, 
	discountRefunds
	FROM amtdb.installment_payments
	)
select *,
sum(amtPaid)over(partition by accountId ORDER  by paidDate asc) as cum_amt_paid
#count(distinct accountId) 
from installment_payments_cte
where accountId in ('114540')
ORDER BY accountId, paidDate