WITH 
kyc_requests_cte as (
	SELECT id, leadId, idNumber, dob, createdAt, updatedAt, gender, _exported_at
	FROM kaleidofin_partner_data.kyc_requests
	)
SELECT count(*)
from kyc_requests_cte