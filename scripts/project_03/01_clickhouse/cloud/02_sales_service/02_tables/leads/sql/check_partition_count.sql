-- Check parts count
SELECT 
    partition,
    count() as part_count,
    sum(rows) as total_rows
FROM system.parts
WHERE database = 'sales-service' 
  AND table = 'leads' 
  AND active = 1
GROUP BY partition
ORDER BY partition DESC;