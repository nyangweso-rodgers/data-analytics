with
wallet_installment_payments_cte as (
	SELECT id, accountId, instalmentScheduleId, ledgerEntryId, paymentType, amountPaid, amountRefunded, paymentDate, refundDate, discountId, paymentId
	#refundId, createdBy, updatedBy, createdAt, updatedAt
	FROM amtdb.wallet_installment_payments
	),
validate_accounts_with_same_payment_dates_cte as (
	select distinct accountId, 
	count(distinct date(paymentDate)) as payment_date_count, 
	sum(amountPaid) as amountPaid
	from wallet_installment_payments_cte
	group by 1
	having payment_date_count <= 5
	)
#accounts_with_no_amortization as 
select *
#count(distinct accountId), sum(amountPaid)
from wallet_installment_payments_cte
#from validate_accounts_with_same_payment_dates_cte
#where accountId = '33448'
where accountId  in ('58395')