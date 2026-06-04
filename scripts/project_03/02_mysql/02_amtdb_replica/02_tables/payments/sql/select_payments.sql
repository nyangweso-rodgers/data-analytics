with
payments_cte as (
	SELECT id, #old_id, paymentTypeId, old_payment_typeId, 
	customerId, 
	#old_customer_id, 
	accountRef, 
	#currencyId, old_currency_id, 
	timestampMade, timestampReceived, paymentRef, 
	#payerNames, bankName, payerNumber, isActive, source, 
	accountId, 
	#old_account_id, 
	createdAt,
	#createdBy, 
	updatedAt, updatedBy, 
	#forexRate, sourceAmountCurrency,
	amount, 
	sum(amount)over(partition by customerId, accountRef order by timestampReceived) as cum_amount
	FROM amtdb.payments
),
agg_payment_refs_payments_cte as ()
select #*
#count(distinct accountRef) as account_ref_count
from payments_cte
#where customerId = '54201'
#where customerId = '47384'
#where accountId = '115539'
#where accountRef = '8582272'
#where accountId = '122044'
#where accountId in ('114540')
#where customerId  = '108801'
#where accountId = '112955'