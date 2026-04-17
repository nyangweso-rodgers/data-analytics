WITH
--------------------- Refund Categories ----------------------------------
refund_categories_cte as (
    SELECT distinct id,
    name,
    row_number() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rnk
    FROM amt.refund_categories
    ) 
select *
from refund_categories_cte
LIMIT 31 OFFSET 0;