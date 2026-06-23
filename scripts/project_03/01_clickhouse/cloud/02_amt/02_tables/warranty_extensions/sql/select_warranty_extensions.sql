WITH
warranty_extensions_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by accountId ORDER BY sync_at desc) as rnk  
        FROM amt.warranty_extensions
        ) where rnk = 1
    )
select *
from warranty_extensions_cte