SELECT --id, "timestamp", user_id, session_id, device_id, device_description, ip_address
count(*)
FROM public.login_history;