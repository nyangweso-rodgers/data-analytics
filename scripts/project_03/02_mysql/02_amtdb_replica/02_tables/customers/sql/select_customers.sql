with
customers_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	identificationNumber, 
	#customerTypeId, 
	#old_customer_type_id, 
	name, 
	phoneNumber,
	#gender, 
	#location, location1, location2, 
	#latitude, longitude, 
	#referralOption, interests, customerSource, eventId, partnerId, 
	#referredById, 
	#shareReferral, email, 
	#referralName, 
	#referralPhoneNumber, 
	#creditCheck, 
	#creditCheckId, 
	#reactivate, referralNationalId, xeroContactId, 
	createdAt, 
	#createdBy, 
	#old_created_by, 
	updatedAt, 
	#updatedBy, 
	#old_updated_by, unLeashedId, vatValue, 
	salesAgents, 
	#netSuiteId, 
	#revenuePin, taxExemptionCertNo, Phone_Number, 
	#identificationSerialNumber
	#documentType, 
	kycDate, 
	dateOfBirth, 
	#alternativePhoneNumber, meta, 
	#nationalIdFrontPic, nationalIdBackPic, 
	salesForceId, 
	#productOfInterest, 
	isMigrated, 
	#ruleId, 
	#sundeskId, 
	walletID
	FROM amtdb.customers
	),
validate_null_saleforce_id_cte as (
	select *
	from customers_cte
	where salesForceId is null
	and companyRegionId = 1
	)
select #*
distinct companyRegionId, isMigrated, count(distinct id) as customer_count
#count(*)
#count(distinct salesForceId)
from customers_cte
#from customer_with_null_saleforce_id_cte
#where id in ()
#where identificationNumber in ()
GROUP BY 1,2 ORDER  BY 1, 2,3 DESC