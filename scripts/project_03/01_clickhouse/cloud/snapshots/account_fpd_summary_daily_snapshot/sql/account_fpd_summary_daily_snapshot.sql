WITH
--------------------- snapshots - Accounts FPD Snapshot ----------------------------------
account_fpd_summary_daily_snapshot_cte as (
    SELECT * 
    FROM snapshots.account_fpd_summary_daily_snapshot
    where first_expected_date = '2026-10-10'
    and is_fpd_amount = 1
    and date(snapshot_date) = '2026-10-11'
    ),
--------------------- agg - Accounts FPD Snapshot ----------------------------------
agg_account_fpd_summary_daily_snapshot_cte as (
    select distinct snapshot_date,
    count(*) as record_count
    from account_fpd_summary_daily_snapshot_cte
    group by 1
    ORDER BY 1 desc
)
select *
--max(snapshot_date), max(_generated_at)
from account_fpd_summary_daily_snapshot_cte
--from agg_account_fpd_summary_daily_snapshot_cte
LIMIT 1000