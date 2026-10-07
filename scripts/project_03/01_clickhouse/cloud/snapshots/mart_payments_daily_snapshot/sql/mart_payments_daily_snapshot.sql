WITH
--------------------- snapshots - payments  ----------------------------------
mart_payments_daily_snapshot_cte as (
    SELECT snapshot_date,
    _generated_at,
    account_id,
    payment_id,
    payment_ref,
    amount,
    timestamp_made
    FROM snapshots.mart_payments_daily_snapshot
    where date
    ) 
select *
from mart_payments_daily_snapshot_cte
limit 1000