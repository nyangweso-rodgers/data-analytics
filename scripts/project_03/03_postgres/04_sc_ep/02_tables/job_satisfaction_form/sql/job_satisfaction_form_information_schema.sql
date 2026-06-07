SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'job_satisfaction_form'
--order by 1