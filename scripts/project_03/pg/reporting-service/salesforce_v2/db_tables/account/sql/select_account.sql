with
accounts_cte as (
	SELECT id, 
	--isdeleted, masterrecordid, 
	"name", "type", parentid, 
	--billingstreet, billingcity, billingstate, billingpostalcode, billingcountry, billinglatitude, billinglongitude, billinggeocodeaccuracy, 
	--shippingstreet, shippingcity, shippingstate, shippingpostalcode, shippingcountry, shippinglatitude, shippinglongitude, shippinggeocodeaccuracy, 
	phone, 
	--website, photourl, industry, numberofemployees, description, ownerid, 
	--createddate, createdbyid, lastmodifieddate, lastmodifiedbyid, systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, 
	sourcesystemidentifier, ispartner, channelprogramname, channelprogramlevelname, 
	--jigsaw, jigsawcompanyid, 
	accountsource, sicdesc, customer_type, acreage, payment_method, water_source, 
	"location", account_number, status, customer_source_account, credit_check_result_status, 
	credit_score, customer_amt_id, date_of_birth_lead, amt_customer_id, income_threshold_l, 
	daily_water_usage_litres_l, water_source_distance_meters_l, category, referral_name, referral_id, 
	referral_phone_number, payment_terms, total_dynamic_head_mtrs, regionid, gender, crb_score, paymentsms, payment_sms_sent, country_code, 
	--xpayment_terms, agent_name, customer_county, customer_kra_pin, customer_to_claim_vat_account, alternative_phone_number, 
	creditcheck_dateupdated, 
	through_partner_customer, 
	new_credit_score, 
	new_credit_check_result_status 
	--_salesforce_id, _synced_at
	FROM salesforce_v2.account
)
select *
from accounts_cte