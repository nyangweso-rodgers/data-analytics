WITH
--------------------- zammad_production - organizations ----------------------------------
organizations_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
        FROM zammad_production.organizations
        ) where rnk = 1
    )
select *
from organizations_cte
LIMIT 1000