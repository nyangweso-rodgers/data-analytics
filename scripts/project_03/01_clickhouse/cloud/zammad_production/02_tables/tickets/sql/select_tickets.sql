WITH
tickets_cte as (
    SELECT * 
    FROM tickets
    )
select distinct type,
count(id) as recordCount
from tickets_cte
group by 1
ORDER BY 2 desc
limit 1000