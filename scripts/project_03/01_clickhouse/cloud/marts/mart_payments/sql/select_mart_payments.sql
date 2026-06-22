WITH
mart_payments_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by payment_id ORDER BY _generated_at desc) as rnk  
        FROM marts.mart_payments
        --FROM test.test_marts_mart_payments -- TEST
        ) --where rnk = 1
    )
select *
from mart_payments_cte 
where account_id ='159027'
ORDER BY timestamp_made