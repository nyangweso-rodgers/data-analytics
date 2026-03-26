WITH
customers_cte as (
    SELECT *
    from (
        select *,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        --FROM sunculture.customers
        FROM amt.customers
        ) where rnk = 1 
    )
select --*
count(*)
--distinct gender, count(distinct id)
from customers_cte