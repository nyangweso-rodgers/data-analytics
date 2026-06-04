with
agent_providers_cte as (
	SELECT name, 
	location, 
	description, 
	created_at, 
	updated_at, 
	provider_id, 
	#companyRegionId, 
	#is_active, 
	#deactivated_at, 
	#parent_provider_id, 
	externalProviderId
	FROM amtdb.agent_providers
	)
select *
from agent_providers_cte