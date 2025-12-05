with
wallet_installment_payments_cte as (
	SELECT id, 
	accountId, 
	instalmentScheduleId, 
	#ledgerEntryId, 
	paymentType, 
	amountPaid, 
	amountRefunded, 
	paymentDate, 
	refundDate, 
	discountId, 
	paymentId, 
	refundId 
	#createdBy, 
	#updatedBy, 
	#createdAt, 
	#updatedAt
	FROM amtdb.wallet_installment_payments
	order by accountId,  paymentDate asc
	)
SELECT *
#distinct accountId, count(*)
#COUNT(distinct accountId)
FROM wallet_installment_payments_cte
#where accountId = '153450'
where accountId = '64275'
#where accountId not in (select distinct accountId FROM amtdb.installment_payments)
#group by 1 order by 2 desc