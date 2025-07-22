
-- The final resulting table will assign a score between 1 and 4 for each of the quantiles (recency, frequency and monetary value)
sql
select distinct(shop_id) as unique_stall,
        ntile(4) over (order by last_sales_date) as rfm_recency,
        ntile(4) over (order by delivery_count) as rfm_frequency,
        ntile(4) over (order by avg_amount) as rfm_monetary
from (select shop_id, 
       max(sales_date) as last_sales_date, -- last delivery date
       count(*) as delivery_count,
       sum(amount) as total_delivery_amount,
       avg(amount) as avg_amount
from transactions.sales
where sales_date between '2020-01-01' and '2020-07-31'
group by 1
order by 1)
```

```sql
-- These individual scores can be combined to create an aggregate score if needed.
select distinct(unique_stall) as unique_stall, rfm_recency*100 + rfm_frequency*100 + rfm_monetary*100 as rfm_combined from (select distinct(shop_id) as unique_stall,
        ntile(4) over (order by last_sales_date) as rfm_recency,
        ntile(4) over (order by delivery_count) as rfm_frequency,
        ntile(4) over (order by avg_amount) as rfm_monetary
from (select shop_id, 
       max(sales_date) as last_sales_date,
       count(distinct sales_date) as delivery_count,
       sum(amount)/26 as avg_amount
from transactions.sales
where sales_date between '2020-06-01' and '2020-06-30'
group by 1
order by 1))