with
accounts_cte as (
	SELECT id, 
	#old_id, accountTypeId, old_payplan, old_account_type_id, 
	customerId, 
	#old_customer_id, paygContractNumber, accountRef, acreage, accountBypass, accountNotes, status, accountBalance, fvreceivable, jsfDate, jsfId, parentAccountId, dispatchDate, expectedStartDate, firstInstallmentDate, 
	#isRevenuePosted, revenuePostedAt, manualDate, revenueReversalAt, externalId, installationId, installationDate, depositPaymentId, fullDepositDate, externalIdSource, 
	createdAt, 
	#createdBy, old_created_by, 
	updatedAt
	#updatedBy, old_updated_by, salesAgents, assignmentId, assignmentDate, netSuiteAccountId, isMigrated, isWalletActive, walletID, creditCheck, creditCheckId
	FROM amtdb.accounts
	),
customers_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	#identificationNumber, customerTypeId, old_customer_type_id, name, 
	#phoneNumber, 
	#gender, location, location1, location2, latitude, longitude, referralOption, interests, customerSource, eventId, partnerId, referredById, shareReferral, email, 
	#referralName, referralPhoneNumber, creditCheck, creditCheckId, reactivate, referralNationalId, xeroContactId, 
	#createdAt, 
	#createdBy, old_created_by, 
	updatedAt
	#updatedBy, old_updated_by, unLeashedId, vatValue, salesAgents, netSuiteId, 
	#revenuePin, taxExemptionCertNo, Phone_Number, 
	#identificationSerialNumber, documentType, kycDate, dateOfBirth, alternativePhoneNumber, meta, nationalIdFrontPic, nationalIdBackPic, 
	#salesForceId, productOfInterest, 
	#isMigrated, ruleId, sundeskId, walletID, optOutOfMessaging
	FROM amtdb.customers
	),
company_regions_cte as (
	SELECT id, 
	region
	#companyName,createdAt, createdBy, updatedAt, updatedBy, defaultCurrencyId
	FROM amtdb.company_regions
	),
accounts_mashup_cte as (
	select distinct accounts_cte.id as accountId,
	company_regions_cte.region,
	accounts_cte.createdAt,
	accounts_cte.updatedAt
	from  accounts_cte
	left join customers_cte on customers_cte.id = accounts_cte.customerId
	left join company_regions_cte on company_regions_cte.id = customers_cte.companyRegionId
	),
check_daily_updates_cte as (
	select distinct 
	max(createdAt), max(updatedAt)
	#date(updatedAt), count(*)
	from accounts_mashup_cte
	where region = 'civ'
	#group by 1 order by 1 desc
	)
select *
from check_daily_updates_cte