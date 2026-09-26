with
auth_identity_cte as (
	SELECT id, user_id, provider, credentials, metadata, provider_id, last_used_at, expires_at, created_at, updated_at
	FROM public.auth_identity
	order by provider_id 
	)
select *
from auth_identity_cte