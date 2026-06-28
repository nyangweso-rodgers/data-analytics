WITH
--------------------- BOQ ----------------------------------
boq_cte as (
    select *
    from (
        SELECT id,
        accountId,
        boqStatus,
        boqDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        FROM amt.boq
    ) WHERE rnk = 1
    )
select --*
distinct boqStatus
from boq_cte
limit 10