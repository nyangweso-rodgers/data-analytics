WITH
account_logs_cte as (
    select *
    from (
    SELECT distinct accountId,
    createdAt,
    updatedAt,
    eventType,
    payload,
    note,
    createdBy
    --row_number() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rnk 
    FROM amt.account_logs FINAL
    ) --where rnk = 1
)
select *
--count(*)
from account_logs_cte
--where accountId in ('77667', '144635', '168646')
--where accountId ='176745'
--WHERE COALESCE(accountId, 0) = '176745'  -- ← Match the ORDER BY expression
WHERE COALESCE(accountId, 0) ='102933'
--where date(updatedAt) > '2026-01-01'
--and eventType not in ('sms')
ORDER BY accountId, updatedAt
limit 1000