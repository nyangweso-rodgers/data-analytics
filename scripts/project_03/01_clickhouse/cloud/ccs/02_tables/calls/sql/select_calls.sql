WITH
--------------------- ccs - calls ---------------------------------- 
calls_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by call_key ORDER BY _synced_at desc) as rnk  
        FROM ccs.calls
        ) where rnk = 1
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
    disposition,
    count(*) as record_count
    from calls_cte
    GROUP BY 1
    ORDER BY 2 desc
)  
select --*
distinct channel
--distinct status
from calls_cte
--from check_duplicates_cte
--from agg_calls_cte
LIMIT 1000