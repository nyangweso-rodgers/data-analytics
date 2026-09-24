SELECT 
    t.table_name,
    s.n_live_tup AS record_count,
    s.n_tup_ins AS total_inserts,
    s.n_tup_upd AS total_updates,
    s.n_tup_del AS total_deletes
FROM information_schema.tables t
LEFT JOIN pg_stat_user_tables s 
    ON t.table_name = s.relname 
    AND t.table_schema = s.schemaname
WHERE t.table_schema = 'kobo_collect'
ORDER BY record_count DESC;