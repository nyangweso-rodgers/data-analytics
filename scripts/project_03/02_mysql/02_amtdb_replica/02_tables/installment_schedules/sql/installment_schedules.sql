with
installment_schedules_cte as (
	SELECT id, 
	#old_id, 
	accountId, 
	#old_account_id, payPlanId, old_payplan_id, 
	customerId, 
	#old_customer_id, 
	installmentType, 
	paymentSequence, 
	#isRevised, isActive, 
	expectedAmount, 
	expectedDate, 
	#status, comment, 
	createdAt 
	#createdBy, old_created_by, updatedAt, updatedBy, ledgerEntryID
	FROM amtdb.installment_schedules
	)
select *,
sum(expectedAmount)over(partition by accountId order by expectedDate asc) as cum_expected_amount
from installment_schedules_cte
#where customerId = '143141'
#where customerId = '140968' # CASH
#where customerId = '114222'
where accountId = '111139'
order by customerId, accountId, paymentSequence 