WITH
account_logs_cte as (
    select *
    from (
        SELECT *,
    row_number() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rnk 
    FROM amt.account_logs 
    ) where rnk =1
    )
select *
from account_logs_cte
where toStartOfMonth(createdAt) >= '2026-03-01'
LIMIT 31