with
core_session_cte as (
	SELECT id, user_id, created_at, anti_csrf_token, key_hashed, auth_identity_id, expires_at, last_active_at
	FROM public.core_session
	)
select count(*)
from core_session_cte