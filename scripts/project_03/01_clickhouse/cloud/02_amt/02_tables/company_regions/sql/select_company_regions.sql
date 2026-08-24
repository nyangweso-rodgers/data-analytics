WITH
--------------------- Company Regions ----------------------------------
company_regions_cte as (
    select *
    from (
            SELECT id,
            region,
            row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
            FROM amt.company_regions
            ) where rnk = 1
        )
select *
from company_regions_cte
LIMIT 1000