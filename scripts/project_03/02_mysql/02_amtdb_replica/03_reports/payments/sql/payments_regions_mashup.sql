with
payments_cte as (
	SELECT id, 
	#old_id, paymentTypeId, old_payment_typeId, 
	customerId, 
	#old_customer_id, 
	accountRef, 
	#currencyId, old_currency_id, amount, 
	timestampMade, timestampReceived, paymentRef, 
	#payerNames, bankName, payerNumber, isActive, source, accountId, parentPaymentId, 
	#isSplitPayment, old_account_id, 
	createdAt, 
	#createdBy, 
	updatedAt
	#updatedBy, forexRate, sourceAmountCurrency
	FROM amtdb.payments
	),
customers_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	#identificationNumber, customerTypeId, old_customer_type_id, name, 
	#phoneNumber, 
	#gender, location, location1, location2, latitude, longitude, referralOption, interests, customerSource, eventId, partnerId, referredById, shareReferral, email, 
	#referralName, referralPhoneNumber, creditCheck, creditCheckId, reactivate, referralNationalId, xeroContactId, 
	createdAt, 
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
payments_regions_mashup_cte as (
	SELECT distinct payments_cte.id,
	company_regions_cte.region,
	timestampMade,
	payments_cte.updatedAt 
	from payments_cte 
	left join customers_cte on customers_cte.id = payments_cte.customerId 
	left join company_regions_cte on company_regions_cte.id = customers_cte.companyRegionId
	),
check_daily_updates_cte as (
	select distinct date(timestampMade),
	count(distinct id)
	from payments_regions_mashup_cte
	where region = 'kenya'
	group by 1
	order by 1 desc
	)
select *
from check_daily_updates_cte