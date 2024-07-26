----------------------- Delivery Notes ------------------------
with
get_monthly_transactions as (
                          SELECT distinct country_code,
                          outlet_id,
                          date_trunc(delivery_date, month) as delivery_month,
                          row_number()over(partition by outlet_id order by date_trunc(delivery_date, month) asc) as delivery_month_index,
                          FROM `kyosk-prod.karuru_scheduled_queries.karuru_dns_daily_revenue` 
                          WHERE  country_code = 'KE' --and territory_id = 'Kawempe'
                          and outlet_id = '0CWH339P55GPA'
                          --order by delivery_month_index
                          ),

select *
from get_monthly_transactionss