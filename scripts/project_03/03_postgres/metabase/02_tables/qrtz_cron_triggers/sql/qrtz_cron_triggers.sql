SELECT sched_name, trigger_name, trigger_group, cron_expression, time_zone_id
FROM public.qrtz_cron_triggers;