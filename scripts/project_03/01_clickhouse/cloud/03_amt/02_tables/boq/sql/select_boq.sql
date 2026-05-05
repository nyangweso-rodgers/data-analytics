WITH
--------------------- BOQ ----------------------------------
boq_cte as (
    SELECT id,
    accountId,
    boqStatus,
    boqDate,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
    FROM amt.boq
    )
select --*
distinct boqStatus
from boq_cte
limit 10

