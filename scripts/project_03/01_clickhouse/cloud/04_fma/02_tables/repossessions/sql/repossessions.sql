WITH
repossessions_cte as (
    SELECT * 
    FROM fma.repossessions
    ) 
select *
from repossessions_cte
where account_id = '136010'
ORDER BY created_at desc
LIMIT 1000