with
cds_cte as (
	SELECT id, cdsId, leadId, cds1CompletionDate, 
	#cds1Tracker, 
	cds2CompletionDate, 
	#cdsSource, 
	#email, mobileNumber, productId, 
	creditCheckStatus, 
	#creditScore, 
	#creditScoreDate, stage, 
	#surveyCompleted, 
	#nextOfKinEmail, nextOfKinGender, lastModifiedById, createdBy, updatedBy, createdAt, updatedAt, isActive, formId, formVersion, deletedAt, deletedBy, archivedAt, archivedBy, lastModifiedAt, lastModifiedBy, 
	is_migrated, 
	#formVersionId, 
	customerId, accountId 
	#cds1CompletionBy, cds2CompletionBy, creditCheckCompletedBy, creditReviewStatus
	#overallRiskAssessmentComment
	FROM `sales-service`.cds
	),
leads_cte as (
	SELECT id, leadId, 
	#firstName, middleName, lastName, 
	mobilePhone, 
	#email, sundeskUserId, 
	idNumber, companyRegionId, leadConvertedDate, 
	#paymentMethod, purchaseDate, agentProviderId, status, leadStatus, leadCategory, referralId, productOfInterest, preferredLanguage, agentId, createdById, lastUpdatedById, throughPartnerLeadId, isReshuffleLead, 
	createdAt, 
	#updatedAt, phoneNumber, alternatePhoneNumber, name, kraPinNumber, companyName, source, paymentTerms, 
	leadAmtCustomerId, 
	#entityType, customerTypeId, leadChannelId, referralType, employeeReferralId, isActive, kycId, tdhId, 
	#deletedAt, deletedBy, leadSourceId, formId, formVersion, archivedAt, archivedBy, lastModifiedAt, lastModifiedBy, 
	is_migrated 
	#leadSourceOrigin
	FROM `sales-service`.leads
	),
cds_leads_mashup_cte as (
	select distinct cds_cte.leadId,
	customerId, accountId, 
	cds1CompletionDate,cds2CompletionDate,creditCheckStatus,
	leads_cte.companyRegionId as companyRegionId,
	leads_cte.leadAmtCustomerId as  leadAmtCustomerId,
	leads_cte.idNumber,
	leads_cte.mobilePhone,
	date(leads_cte.createdAt) as leadCreatedDate,
	date(leadConvertedDate) as leadConvertedDate,
	leads_cte.is_migrated as leadIsMigrated,
	cds_cte.is_migrated as cdsIsMigrated
	from cds_cte
	left join leads_cte on leads_cte.leadId = cds_cte.leadId
	where customerId is null
	),
check_cds_with_null_accountId_cte as (
	select *
	from cds_leads_mashup_cte
	where accountId is null
	and leadAmtCustomerId is not null
	)
SELECT *
#count(*), count(distinct leadId)
from cds_leads_mashup_cte
#from check_cds_with_null_accountId_cte
#where leadId = '00QPz00000XpxRFMAZ'
#where leadAmtCustomerId is not null
limit 1000