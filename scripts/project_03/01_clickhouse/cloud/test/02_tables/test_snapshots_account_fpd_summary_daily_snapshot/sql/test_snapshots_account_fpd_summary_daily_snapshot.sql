WITH
--------------------- snapshots - fpd summary (latest row per account) --------
account_fpd_summary_cte as (
    select *
    from (
        SELECT *,
        row_number() over (partition by accountId order by snapshot_date desc) as rnk
        FROM test.test_snapshots_account_fpd_summary_daily_snapshot
    ) --where rnk = 1
)
select distinct snapshot_date
from account_fpd_summary_cte
limit 1000