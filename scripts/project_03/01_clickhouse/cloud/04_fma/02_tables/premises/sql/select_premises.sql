WITH
--------------------- Premises ----------------------------------
premises_cte as (
    select *
    from (
        SELECT distinct id,
    account_id,
    premise_name,
    customer_id,
    premise_type_id,
    substate_id,
    town,
    row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
    FROM fma.premises
    ) where rnk = 1
    )
select *
from premises_cte
limit 100
