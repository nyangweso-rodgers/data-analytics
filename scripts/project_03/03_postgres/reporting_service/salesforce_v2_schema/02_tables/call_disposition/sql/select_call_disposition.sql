with
call_disposition_cte as (
	SELECT id, 
	--ownerid, 
	--isdeleted, 
	"name", createddate, createdbyid, lastmodifieddate, lastmodifiedbyid, 
	--systemmodstamp, lastactivitydate, 
	--lastvieweddate, lastreferenceddate, 
	contact, disposition_category, call_direction, create_a_ticket, reported_issues, disposition_sub_catergory, description, call_status
	--ptp_id, # all NULL
	--customer_notes, # all NULL 
	--disposition_status, # all NULL
	--_salesforce_id, _synced_at
	FROM salesforce_v2.call_disposition
	),
contact_cte as (
	SELECT id, 
	--isdeleted, 
	--masterrecordid, 
	accountid, lastname, firstname, 
	--salutation, 
	name, 
	--mailingstreet, mailingcity, mailingstate, mailingpostalcode, mailingcountry, 
	--mailinglatitude, mailinglongitude, mailinggeocodeaccuracy, 
	phone, 
	--fax, 
	mobilephone, reportstoid, 
	email, 
	--title, 
	--department, # all NULL 
	--leadsource, # all NULL 
	--ownerid, 
	--createddate, createdbyid, lastmodifieddate, lastmodifiedbyid, 
	--systemmodstamp, lastactivitydate, lastcurequestdate, lastcuupdatedate, 
	--lastvieweddate, lastreferenceddate, emailbouncedreason, emailbounceddate, isemailbounced, 
	--photourl, jigsaw, jigsawcontactid, ispriorityrecord, 
	id_number, gender, customer_source, 
	--preferred_language, 
	date_of_birth, referral_name, referral_phone_number, referral_id, customer_type, "location", "type", country_code, connex_id
	--_salesforce_id, _synced_at
	FROM salesforce_v2.contact
	),
call_disposition_mashup_cte as (
	select call_disposition_cte.*,
	mobilephone,
	phone,
	accountid,
	referral_id,
	connex_id
	from call_disposition_cte 
	left join contact_cte on contact_cte.id = call_disposition_cte.contact 
	)
select --*
--distinct call_direction 
distinct disposition_category, disposition_sub_catergory
from call_disposition_mashup_cte
--where call_direction = 'Outbound'
order by 1,2