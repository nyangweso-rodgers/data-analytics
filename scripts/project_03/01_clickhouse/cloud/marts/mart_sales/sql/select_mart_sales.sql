WITH
mart_sales_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by account_id ORDER BY _generated_at desc) as rnk  
        FROM marts.mart_sales
    ) where rnk = 1
    )
select *
from mart_sales_cte
LIMIT 31 OFFSET 0;