WITH 
next_of_kin_details_cte as(
	SELECT id, leadId, firstName, lastName, phoneNumber, alternativePhoneNumber, gender, idNumber, relationship, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.next_of_kin_details
	)
select count(*)
from next_of_kin_details_cte