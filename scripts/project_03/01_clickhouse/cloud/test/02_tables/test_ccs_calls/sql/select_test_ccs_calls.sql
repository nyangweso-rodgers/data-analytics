WITH
--------------------- ccs - calls ---------------------------------- 
calls_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by call_key ORDER BY _synced_at desc) as rnk  
        FROM test.test_ccs_calls
        ) where rnk = 1
        --and date(call_date) = '2026-08-10'
        and queue_name = 'PAYG(Outbound)'
    ),
--------------------- calls - duplicates ---------------------------------- 
check_duplicates_cte as (
    select distinct 
    --call_key,
    call_id,
    count(*) as record_count
    from calls_cte
    GROUP BY 1
    HAVING record_count > 1
),
--------------------- calls - agg ----------------------------------
agg_calls_cte as (
    select distinct
    --queue_name,
    tags,
    --disposition,
    --campaign_name, -- all NULL
    --status,
    --channel,
    --answered_by, -- all NULL
    count(*) as record_count
    from calls_cte
    GROUP BY 1
    ORDER BY 2 DESC
)  
select *
--distinct queue_name
--distinct status
--from calls_cte
--from check_duplicates_cte
from agg_calls_cte
LIMIT 1000