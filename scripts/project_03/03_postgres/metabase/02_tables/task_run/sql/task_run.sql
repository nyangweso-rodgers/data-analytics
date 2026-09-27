SELECT --id, run_type, entity_type, entity_id, started_at, ended_at, status, process_uuid, updated_at, notification_id
count(*)
FROM public.task_run;