with
lead_cte as (
	SELECT distinct 
	id, 
	--isdeleted, masterrecordid, lastname, firstname, salutation, name, title, company, street, city, state, postalcode, country, latitude, longitude, geocodeaccuracy, 
	--phone, 
	mobilephone, 
	--website, photourl, 
	leadsource, 
	--status, industry, rating, 
	--numberofemployees, ownerid, isconverted, converteddate, convertedaccountid, convertedcontactid, convertedopportunityid, isunreadbyowner, 
	createddate, createdbyid, lastmodifieddate, 
	--lastmodifiedbyid, systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, jigsaw, jigsawcontactid, emailbouncedreason, emailbounceddate, ispriorityrecord, acreage, 
	--country_code, date_of_birth, gender, 
	lead_amt_customer_id, 
	--installation_date, lead_category, lead_channel, "location", payment_method, preferred_language, product_del, purchase_date, water_source_distance, 
	--water_source, leadcap_facebook_lead_id, customer_type, lead_model_category, id_number, call_back_date, follow_up_date, product, kyc_status, 
	agent, 
	--payment_terms, 
	referral_name, referral_id,
	--income_threshold, last_updated_by, daily_water_usage, lead_source_other_comment, total_dynamic_head, smileidentity_json, 
	referral_phone_number, 
	--custom_opportunity_name, 
	--smsmessage, contact_external_id_source, contactregionid, opportunitypayplanid, old_amt_customer_id, other_phone, 
	lead_date_created,
	--number_of_units_lead, kra_pin, customer_to_claim_vat, 
	--customer_product_of_interest, 
	referral_source_application, 
	--agent_referral_smsbody, through_partner_lead, unique_phone_number, through_partner_customer, 
	referral_lead_id 
	--cds1tracker, 
	--cds_status, survey_stat, sadm_account, sadm_cds_id, sadm_customer, sadm_kyc_date, employee_id, employee_name, employee_phone, is_lead_employed, auto_assignment_date, extagentshopname, 
	--extagentreferral_code, extagentprovider_region, extagentprovider_name, extagentphone_number, extagentname, extagentid, _salesforce_id, _synced_at
	FROM salesforce_v2."lead"
	),
agent_c_cte as (
	SELECT id, 
	--ownerid, isdeleted, 
	"name", createddate, createdbyid, 
	--lastmodifieddate, lastmodifiedbyid, systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, 
	mobilephone, 
	--preferred_language, 
	status, agent_type, 
	--referral_code, 
	supervisor, amt_employee_id, employee_id_number, related_user_account, primary_role, agent_county, sales_cluster, agent_constituency, agent_village, 
	--is_ussd_active, 
	agent_department
	--is_salesapp_active, login_time, otp_code, otp_expirytime, last_auto_assigned_time, delta_since_last_assigned, agent_country_derived, _salesforce_id, _synced_at
	FROM salesforce_v2.agent_c
	),
leads_mashup_cte as (
	select lead_cte.*,
	amt_employee_id,
	employee_id_number,
	agent_type,
	primary_role
	from lead_cte
	left join agent_c_cte on agent_c_cte.id = lead_cte.agent 
	)
select *
--count(*)
from leads_mashup_cte
--where referral_id is not null 
where id in ()