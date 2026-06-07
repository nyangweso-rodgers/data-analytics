WITH
bd_sales_cte as (
    SELECT distinct account_id,
    account_ref,
    product,
    units,
    dispatch_date,
    account
    FROM manual_data_dump.bd_sales
    )
select *
from bd_sales_cte