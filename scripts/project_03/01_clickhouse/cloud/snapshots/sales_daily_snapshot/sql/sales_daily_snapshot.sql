WITH
sales_daily_snapshot_cte as (
    SELECT * 
    FROM snapshots.sales_daily_snapshot
    ) 
select --*
distinct snapshot_date
from sales_daily_snapshot_cte
LIMIT 31 OFFSET 0;