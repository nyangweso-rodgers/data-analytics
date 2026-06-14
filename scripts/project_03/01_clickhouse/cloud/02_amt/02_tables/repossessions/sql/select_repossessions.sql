WITH
--------------------- Repossessions ----------------------------------
repossessions_cte as (
    select *
    from (
        SELECT id,
        accountId,
        amount,
        repossessionType,
        createdAt,
        updatedAt,
        sync_at,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.repossessions
     ) where rnk = 1
    )
select --*
distinct repossessionType, count(distinct accountId)
from repossessions_cte
GROUP BY 1 ORDER BY 2 desc
limit 100