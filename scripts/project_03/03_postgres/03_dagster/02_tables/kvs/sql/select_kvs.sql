SELECT id, "key", value
FROM public.kvs
--where key like 'etl_sync_state:mysql:sales-service:%'
order by key