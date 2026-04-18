WITH
--------------------- Premise Details ----------------------------------
premise_details_cte as (
    select *
    from (
        SELECT distinct id,
        premise_id,
        latitude,
        longitude,
        gps,
        district,
        county,
        subcounty,
        parish,
        village,
    row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
    FROM fma.premise_details
    ) where rnk = 1
    )
select *
from premise_details_cte
limit 100
