SELECT --id, task, db_id, started_at, ended_at, duration, task_details, status, run_id, logs
count(*)
FROM public.task_history;