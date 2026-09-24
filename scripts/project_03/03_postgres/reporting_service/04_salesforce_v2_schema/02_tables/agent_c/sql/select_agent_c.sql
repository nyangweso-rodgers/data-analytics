with
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
	)
select *
from agent_c_cte