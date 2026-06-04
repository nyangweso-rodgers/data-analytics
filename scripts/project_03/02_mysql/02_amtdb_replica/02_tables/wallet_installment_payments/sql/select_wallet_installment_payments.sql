with
wallet_installment_payments_cte as (
	SELECT id, 
	accountId, 
	instalmentScheduleId, 
	ledgerEntryId, 
	paymentType, 
	amountRefunded, 
	refundDate, 
	discountId, 
	paymentId, 
	refundId,
	paymentDate, 
	amountPaid
	#createdBy, updatedBy, createdAt, updatedAt
	FROM amtdb.wallet_installment_payments
	#where paymentType = '1' 
	order by accountId, instalmentScheduleId
	)
select *,
sum(amountPaid)over(partition by accountId order by paymentDate asc) as cum_amount_paid
from wallet_installment_payments_cte
where accountId = '117039'
#and paymentType = '1' # discount payment
#and paymentType = '0'
order by paymentDate