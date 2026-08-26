WITH
--------------------- snapshots -accounts  ----------------------------------
mart_accounts_daily_snapshot_cte as (
    SELECT * 
    FROM snapshots.mart_accounts_daily_snapshot
    ), 
agg_snapshots_cte as (
    select distinct snapshot_date,_generated_at, count(*) as record_count
    from mart_accounts_daily_snapshot_cte
    GROUP BY 1,2
    ORDER BY 1 DESC
    )
select *
from agg_snapshots_cte
LIMIT 1000