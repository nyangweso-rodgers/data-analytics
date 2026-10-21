WITH
--------------------- snapshots - mart_accounts_daily_snapshot  ----------------------------------
mart_accounts_daily_snapshot_cte as (
    SELECT * 
    FROM snapshots.mart_accounts_daily_snapshot
    --where companyRegion = 'kenya'
    --and date(snapshot_date) = '2026-10-12'
    --where date(snapshot_date) between '2026-09-05' and '2026-09-06'
    --where snapshot_date = '2026-09-12 20:59:12.000'
    --and accountType = 'PAYG'
    --and status not in ('No Deposit', ' No Deposit', 'Full Deposit', 'On Hold', 'InActive', 'Partial Deposit', 'Rejected')
    --and status in ('Arrears')
    --and status = 'Arrears' and days_late_current <= 0
    --and days_late_current > 0
    --where accountRef = 'P052426049G-06'
    ), 
--------------------- validation - mart_accounts_daily_snapshot  ----------------------------------
validate_mart_accounts_daily_snapshot_cte as (
    SELECT distinct 
    --max(snapshot_date) as max_snapshot_date, 
    --max(_generated_at) as max_generated_at
    PaidAmount
    from mart_accounts_daily_snapshot_cte
),
--------------------- agg - mart_accounts_daily_snapshot  ----------------------------------
agg_mart_accounts_daily_snapshot_cte as (
    select distinct --companyRegion,
    --account_id,
    --accountRef,
    --product,
    days_late_current,
    --snapshot_date,
    --date(snapshot_date) as snapshot_date,
    --status,
    --days_late_current,
    --snapshot_date,_generated_at, 
    count(*) as record_count
    from mart_accounts_daily_snapshot_cte
    GROUP BY 1
    ORDER BY 1
    )
select *
--distinct days_late_current,
--count(*) as record_acount, count(distinct account_id) as account_id_count
--from mart_accounts_daily_snapshot_cte
from validate_mart_accounts_daily_snapshot_cte
--from agg_mart_accounts_daily_snapshot_cte
--from account_fpd_summary_daily_snapshot_cte
--where accountRef = '24213241'
--where account_id in (select * from account_fpd_summary_daily_snapshot_cte)
LIMIT 1000