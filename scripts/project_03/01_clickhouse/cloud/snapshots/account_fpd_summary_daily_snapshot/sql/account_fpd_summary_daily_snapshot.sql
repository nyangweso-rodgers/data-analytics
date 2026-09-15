WITH
account_fpd_summary_daily_snapshot_cte as (
    SELECT * 
    FROM snapshots.account_fpd_summary_daily_snapshot
    where first_expected_date = '2026-09-10'
    and is_fpd_amount = 1
    )
select count(*)
--distinct snapshot_date
from account_fpd_summary_daily_snapshot_cte
ORDER BY 1 DESC
LIMIT 1000