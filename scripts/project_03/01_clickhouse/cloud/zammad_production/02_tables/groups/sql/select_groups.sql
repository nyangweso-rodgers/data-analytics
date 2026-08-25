WITH
--------------------- groups ----------------------------------
groups_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
        FROM zammad_production.groups
        ) where rnk = 1
    ),
--------------------- agg - groups ----------------------------------
agg_groups_cte as (
    select distinct name,
    max(created_at) as created_at,
    max(updated_at) as max_updated_at,
    count(*) as record_count
    from groups_cte
    group by 1
    ORDER BY 3 desc
)
select *
--from groups_cte
from agg_groups_cte
LIMIT 1000 