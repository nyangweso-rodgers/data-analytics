WITH
--------------------- Regions ----------------------------------
regions_cte as (
    select *
    from (
        SELECT distinct id,
        region_name,
        country_id,
    row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
    FROM fma.regions
    ) where rnk = 1
    )
select *
from regions_cte
limit 100
