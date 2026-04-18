with
leads_cte as (
	SELECT id, 
	leadId, 
	#firstName, 
	#middleName, 
	#lastName, 
	mobilePhone, 
	REPLACE(mobilePhone, '+', '') AS cleanMobilePhone,
	#email, sundeskUserId, 
	idNumber, 
	companyRegionId, 
	#leadConvertedDate, paymentMethod, purchaseDate, 
	agentProviderId, 
	status, 
	leadStatus, 
	#leadCategory, 
	referralId, 
	#productOfInterest, preferredLanguage, 
	agentId, 
	createdById, 
	#lastUpdatedById, 
	#throughPartnerLeadId, 
	#isReshuffleLead, 
	createdAt, 
	#updatedAt, 
	#phoneNumber, 
	#alternatePhoneNumber, 
	#name, kraPinNumber, companyName, 
	source, 
	#paymentTerms, 
	leadAmtCustomerId, 
	#entityType, customerTypeId, 
	leadChannelId, 
	referralType, 
	employeeReferralId, 
	#isActive, 
	#kycId, tdhId, deletedAt, deletedBy, 
	leadSourceId, 
	#formId, formVersion, archivedAt, archivedBy, lastModifiedAt, lastModifiedBy, 
	is_migrated
	FROM `sales-service`.leads
	)/*,
duplicate_leads_cte as (
	select cleanMobilePhone, 
	count(distinct id) as lead_count
	from leads_cte
	#where mobilePhone in ('254728513982', '+254728513982')
	group by 1
	having lead_count > 1
	),
referral_leads_cte as (
	select *
	#count(*), max(createdAt), min(createdAt)
	#length(referralId) as referral_id_length
	from leads_cte 
	where is_migrated = 0
	and leadSourceId in (62, 4) # Refer & Earn / Refer and Earn
	and companyRegionId = 1 # kenya
	# companyRegionId = 3 # uganda
	#and referralId is null
	),
leads_to_be_tagged_cte as (
	select *
	from leads_cte 
	where agentId is null
	and companyRegionId =1
	and DATEDIFF(CURRENT_DATE(), date(createdAt)) >= 120
	and leadAmtCustomerId is null
	and status <> 'CONVERTED'
	#and leadId = '00QPz00000VD2q9MAD'
	)
*/
select *
#distinct referralType, count(distinct leadId)
#distinct leadSourceId, count(distinct leadId) 
#count(*)
from leads_cte
#from duplicate_leads_cte
#from referral_leads_cte
#from leads_on_amt_cte
#from leads_to_be_tagged_cte
where leadSourceId = 
#where idNumber in ()
#group by 1 order by 2 desc
limit 1000