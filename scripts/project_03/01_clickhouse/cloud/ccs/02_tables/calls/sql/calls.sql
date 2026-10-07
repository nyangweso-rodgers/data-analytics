WITH
--------------------- ccs - calls ---------------------------------- 
calls_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by call_key ORDER BY _synced_at desc) as rnk  
        FROM ccs.calls
        ) where rnk = 1
        --and date(call_date) = '2026-07-29'
        and queue_name = 'PAYG(Outbound)'
        --and queue_name = 'Customer Support (Inbound)'
        and country = 'kenya'
        --and agent_name = 'Jane Aisu'
        --and caller_id like '%48018035%'
    ),
--------------------- ccs - calls_report_cte ---------------------------------- 
calls_report_cte as (
    select distinct unique_id,
    queue_key,
    queue_name,
    caller_id,
    called_number,
    call_date,
    talk_date,
    hangup_date
    is_answered,
    disposition,
    wait_duration,
    agent_key,
    agent_name,
    agent_email,
    tags,
    _synced_at
    from calls_cte
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
    select distinct --country,
    queue_name,
    --agent_name,
    --tags,
    --disposition,
    --campaign_name, -- all NULL
    --status,
    --channel,
    --answered_by, -- all NULL
    count(*) as record_count
    --min(call_date), max(call_date), max(_synced_at)
    from calls_cte
    GROUP BY 1
    ORDER BY 1-- DESC
)  
select *
--distinct queue_name
--distinct status
--from calls_cte
from calls_report_cte
--from check_duplicates_cte
--from agg_calls_cte
LIMIT 1000