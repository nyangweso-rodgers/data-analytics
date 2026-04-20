--CREATE VIEW amt.vw_agent_providers AS
WITH
agent_providers_cte as (
    select name, location,
    created_at,
    updated_at,
    deactivated_at,
    provider_id,
    companyRegionId,
    is_active,
    parent_provider_id
    from (
        SELECT *,
        row_number() OVER (partition by provider_id ORDER BY updated_at DESC) as rnk 
        FROM sunculture.agent_providers
    ) WHERE rnk = 1
    )
select *
from agent_providers_cte