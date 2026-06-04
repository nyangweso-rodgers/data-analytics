with
customer_referrals_cte as (
	SELECT referral_id, 
	agent_id, 
	customer_first_name, 
	customer_last_name, 
	customer_phone_number, 
	customer_id_number, 
	status, 
	location, 
	created_at, 
	#updated_at, 
	referral_point, 
	agent_provider_id, 
	#has_heard_of_sunculture, 
	#purchase_date, 
	#customer_id, 
	referral_type
	FROM amtdb.customer_referrals
),
agent_providers_cte as (
	SELECT name, 
	location, 
	description, 
	#created_at, 
	#updated_at, 
	provider_id, 
	#companyRegionId, 
	#is_active, 
	#deactivated_at, 
	#parent_provider_id, 
	externalProviderId
	FROM amtdb.agent_providers
	),
customer_referrals_mashup_cte as (
	select customer_referrals_cte.*,
	REPLACE(customer_phone_number, '+', '') as customer_phone_number_clean,
	name,
	description,
	provider_id,
	externalProviderId
	from customer_referrals_cte
	left join agent_providers_cte on agent_providers_cte.provider_id = customer_referrals_cte.agent_provider_id
	order by created_at desc
	)
select *
#distinct customer_id_number
#count(*)
from customer_referrals_mashup_cte
#where customer_phone_number in ('254724094412')
#where customer_id_number in ()