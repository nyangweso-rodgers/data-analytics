with
accounts_cte as (
	SELECT id, 
	#old_id, 
	accountTypeId, 
	#old_payplan, old_account_type_id, 
	customerId, 
	#old_customer_id, paygContractNumber, 
	accountRef, 
	#acreage, accountBypass, accountNotes, 
	status, 
	accountBalance, 
	#fvreceivable, jsfDate, jsfId, parentAccountId, dispatchDate, expectedStartDate, 
	#firstInstallmentDate, isRevenuePosted, revenuePostedAt, manualDate, revenueReversalAt, externalId, installationId, installationDate, depositPaymentId, fullDepositDate, externalIdSource, 
	createdAt,
	#createdBy, old_created_by, 
	updatedAt
	#updatedBy, old_updated_by, 
	#salesAgents, assignmentId, assignmentDate, netSuiteAccountId, isMigrated, isWalletActive, walletID, 
	#creditCheck, creditCheckId
	FROM amtdb.accounts
),
agg_account_status_cte as (
	select distinct status,
	#count(distinct id) as account_id_count,
	count(distinct accountRef) as account_ref_count
	from accounts_cte
	where accountRef in (
)
	group by 1
	order by 2 desc
	)
/*
validate_credit_check_cte as (
	select distinct customerId,
	id as accountId,
	creditCheck
	#count(*)
	from accounts_cte
	#GROUP BY 1
	#ORDER BY 2 DESC
	)
*/
select *
#count(*)
#sum(account_id_count)
#distinct creditCheck
#from accounts_cte
from agg_account_status_cte
#from validate_credit_check_cte
#where creditCheck is not null