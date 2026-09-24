with
sms_providers_cte as (
	SELECT id, country, provider, "isActive", "isPrimary", "createdAt", "updatedAt"
	FROM public.sms_providers
	)
select *
from sms_providers_cte
limit 100