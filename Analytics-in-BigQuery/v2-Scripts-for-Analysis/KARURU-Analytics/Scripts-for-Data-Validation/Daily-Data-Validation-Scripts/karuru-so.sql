---------------------- KARURU - 
----------- Sales Order --------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name  order by last_modified_date desc) as index
                            FROM `kyosk-prod.karuru_reports.sales_order` so
                            where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                            --WHERE date(created_date) between '2023-07-01' and '2023-07-31'
                            and date(created_date) >= date_sub(current_date, interval 4 month)
                            and is_pre_karuru = false
                            ),
so_summary as (
                select distinct created_date,
                so.id,
                name,
                order_status,
                outlet_id,
                territory_id,
                --outlet.name as outlet_name,
                --outlet.outlet_code,
                --outlet.phone_number
                from sales_order_with_index so--, unnest(outlet) o
                where index = 1
                )
select *
from so_summary
where name = 'SOJSMX52023'