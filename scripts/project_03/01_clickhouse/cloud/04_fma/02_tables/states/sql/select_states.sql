WITH
--------------------- States ----------------------------------
states_cte as (
    select *
    from (
        SELECT distinct id,
        state_type,
        state_name,
        country_id,
        region_id,
    row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
    FROM fma.states
    ) where rnk = 1
    ),
--------------------- County ----------------------------------
county_cte as (
    select distinct id,
    state_type,
    state_name,
    country_id,
    region_id
    from states_cte
    where state_type = 'County'
)
select *
--from states_cte
from county_cte
limit 100
