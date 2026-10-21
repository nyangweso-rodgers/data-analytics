WITH
--------------------- test - test_snapshots_mart_accounts_daily_snapshot  ----------------------------------
test_snapshots_mart_accounts_daily_snapshot_cte as (
    SELECT * 
    FROM test.test_snapshots_mart_accounts_daily_snapshot
    WHERE date(snapshot_date) = '2026-10-21'
    ),
--------------------- validation - test_snapshots_mart_accounts_daily_snapshot  ----------------------------------
validate_test_snapshots_mart_accounts_daily_snapshot_cte as (
    SELECT 
    max(snapshot_date) as max_snapshot_date, 
    max(_generated_at) as max_generated_at,
    count(*) as record_count,
    count(distinct account_id) as account_id_count
    --Total_Paid,
    --PaidAmount -- deleted column from the source 
    from test_snapshots_mart_accounts_daily_snapshot_cte
),
--------------------- agg - test_snapshots_mart_accounts_daily_snapshot  ----------------------------------
agg_test_snapshots_mart_accounts_daily_snapshot_cte as (
    select distinct snapshot_date,
    count(*) as record_count
    from test_snapshots_mart_accounts_daily_snapshot_cte
    group by 1
    order by 1 desc
    )
select *
--from test_snapshots_mart_accounts_daily_snapshot_cte
from validate_test_snapshots_mart_accounts_daily_snapshot_cte
--from agg_test_snapshots_mart_accounts_daily_snapshot_cte
LIMIT 1000