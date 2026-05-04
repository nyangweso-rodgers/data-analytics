with
sales_cte as (
        SELECT account_id, 
        sale_date,
        updatedAt
        FROM amtdb.sales
        )
select *
#min(sale_date), min(updatedAt), max(sale_date), max(updatedAt) 
from sales_cte
where account_id ='293'