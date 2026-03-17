with
installment_schedules_cte as (
	SELECT id, 
	#old_id, 
	accountId, 
	#old_account_id, 
	payPlanId, 
	#old_payplan_id, 
	customerId, 
	#old_customer_id, 
	installmentType, 
	paymentSequence, 
	#isRevised, 
	#isActive, 
	expectedAmount, 
	expectedDate
	#status 
	#comment 
	#createdAt 
	#createdBy, 
	#old_created_by, 
	#updatedAt, 
	#updatedBy, 
	#ledgerEntryID
	FROM amtdb.installment_schedules
	order by customerId, accountId, paymentSequence 
	),
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
	#createdAt, 
	#date(createdAt) as created_at,
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	#ledgerEntryID, 
	discountRefunds
	FROM amtdb.installment_payments
	),
accounts_cte as (
	SELECT id, 
	#old_id, 
	accountTypeId, 
	#old_payplan, 
	#old_account_type_id, 
	#customerId, 
	#old_customer_id, 
	#paygContractNumber, 
	accountRef, 
	#acreage, 
	#accountBypass, 
	#accountNotes, 
	status, 
	#accountBalance, 
	#fvreceivable, 
	jsfDate, 
	#jsfId, 
	#parentAccountId, 
	dispatchDate 
	#expectedStartDate, 
	#firstInstallmentDate, 
	#isRevenuePosted, 
	#revenuePostedAt, 
	#manualDate, 
	#revenueReversalAt, 
	#externalId, 
	# installationId, # all NULL
	# installationDate, # all NULL 
	#depositPaymentId, 
	#fullDepositDate, 
	#externalIdSource, 
	#createdAt, 
	#createdBy, 
	#old_created_by, 
	#updatedAt, 
	#updatedBy, 
	#old_updated_by, 
	#salesAgents, 
	#assignmentId, 
	#assignmentDate, 
	#netSuiteAccountId
	FROM amtdb.accounts
	),
account_type_cte as (
	SELECT id, 
	#old_id, 
	accountType 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy
	FROM amtdb.account_types
	),
installment_schedules_mashup_cte as (
	select distinct date(dispatchDate) as dispatchDate,
	date(jsfDate) as jsfDate,
	installment_schedules_cte.id as installment_schedule_id,
	customerId,
	installment_schedules_cte.accountId,
	accountRef,
	account_type_cte.accountType,
	accounts_cte.status as account_status,
	paymentSequence,
	installment_schedules_cte.expectedDate,
	max(date(paidDate)) as paidDate,
	expectedAmount,
	sum(amtPaid) as amtPaid
	from installment_schedules_cte 
	left join installment_payments_cte on installment_payments_cte.instalmentScheduleId = installment_schedules_cte.id
	left join accounts_cte on accounts_cte.id = installment_schedules_cte.accountId
	left join account_type_cte on account_type_cte.id = accounts_cte.accountTypeId 
	order by customerId, accountId, paymentSequence 
	)
select * 
#from installment_schedules_cte
from installment_schedules_mashup_cte
#where customerId = '143141'
#where customerId = '140968' # CASH
#where customerId = '114222'
where accountId = '64275'