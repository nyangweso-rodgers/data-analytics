with
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
customer_regions_mashup_cte as (
	select distinct customers_cte.id as customerId,
	company_regions_cte.region,
	customers_cte.updatedAt
	from customers_cte
	left join company_regions_cte on company_regions_cte.id = customers_cte.companyRegionId
	),
check_daily_updates_cte as (
	select distinct date(updatedAt), count(*)
	from customer_regions_mashup_cte
	where region = 'civ'
	group by 1 order by 1 desc
	)
SELECT *
from check_daily_updates_cte
limit 100