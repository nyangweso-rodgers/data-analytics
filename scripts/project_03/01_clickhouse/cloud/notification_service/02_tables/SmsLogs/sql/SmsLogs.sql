WITH
--------------------- notification_service - SmsLogs ---------------------------------- 
smslogs_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by id ORDER BY updatedAt desc) as rnk   
        FROM notification_service.SmsLogs
        ) where rnk = 1
    ),
--------------------- notification_service - smslogs_report_cte ---------------------------------- 
smslogs_report_cte as (
    select distinct id,
    region,
    app,
    recipient,
    responce,
    messageId,
    payload,
    createdAt,
    sender,
    failureReason
    FROM smslogs_cte
    ),
--------------------- agg - smslogs_cte ---------------------------------- 
agg_smslogs_cte as (
    select distinct
    --app,
    --region,
    --provider,
    --status,
    count(*) as record_count,
    count(distinct id) as id_count
    from smslogs_cte
    group by 1
    ORDER BY 2 desc
) 
select --*
distinct sender
from smslogs_cte
--from agg_smslogs_cte
LIMIT 1000