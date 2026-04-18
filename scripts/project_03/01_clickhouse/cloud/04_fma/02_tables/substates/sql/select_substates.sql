WITH
--------------------- Substates ----------------------------------
substates_cte as (
    select *
    from (
        SELECT distinct id,
        state_id,
        substate_type,
        substate_name,
    row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
    FROM fma.substates
    ) where rnk = 1
    ),
--------------------- Sub County ----------------------------------
sub_county_cte as (
    select distinct id,
    state_id,
    substate_type,
    substate_name
    from substates_cte
    where substate_type = 'Sub County'
)
select --*
distinct substate_type
from substates_cte
limit 100
