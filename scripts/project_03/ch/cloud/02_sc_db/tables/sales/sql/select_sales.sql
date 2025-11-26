with 
sales_cte as (
        select *
        from (
                select account_id,
                toDateTimeOrNull(sale_date) as sale_date,,
                updatedAt,
                row_number() OVER (partition by account_id ORDER BY updatedAt DESC) as rnk 
                from sunculture.sales
        )
        where rnk = 1 
        )
select * 
from sales_cte
where account_id = '21343'