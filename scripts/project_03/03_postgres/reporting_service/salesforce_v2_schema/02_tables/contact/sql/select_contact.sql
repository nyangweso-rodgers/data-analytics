with
contact_cte as (
	SELECT id, isdeleted, masterrecordid, accountid, lastname, firstname, salutation, name, mailingstreet, mailingcity, mailingstate, mailingpostalcode, mailingcountry, mailinglatitude, mailinglongitude, mailinggeocodeaccuracy, phone, fax, mobilephone, reportstoid, email, title, department, leadsource, ownerid, createddate, createdbyid, lastmodifieddate, lastmodifiedbyid, systemmodstamp, lastactivitydate, lastcurequestdate, lastcuupdatedate, lastvieweddate, lastreferenceddate, emailbouncedreason, emailbounceddate, isemailbounced, photourl, jigsaw, jigsawcontactid, ispriorityrecord, id_number, gender, customer_source, preferred_language, date_of_birth, referral_name, referral_phone_number, referral_id, customer_type, "location", "type", country_code, connex_id, _salesforce_id, _synced_at
	FROM salesforce_v2.contact
	)
select *
from contact_cte