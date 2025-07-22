with   
order_cte as (
              select date '2024-12-01' as order_date, 1 as order_id, 1 as customer_id
              union all (select date '2024-12-02' as order_date, 2 as order_id, 2 as customer_id)
              ),
deliveries_cte as (
                    select date '2024-12-02' as delivery_date, 2 as order_id, 2 as delivery_id, 2 as customer_id
                    union all (select date '2024-12-03' as delivery_date, 3 as order_id, 3 as delivery_id, 3 as customer_id)
                    )
select distinct o.order_date,
d.delivery_date,
o.customer_id,
o.order_id,
d.delivery_id
from order_cte o
full outer join deliveries_cte d on o.order_id = d.order_id
