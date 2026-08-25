WITH
dispositions_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
        FROM zammad_production.dispositions
        ) where rnk = 1
    ) 
select *
from dispositions_cte
LIMIT 1000