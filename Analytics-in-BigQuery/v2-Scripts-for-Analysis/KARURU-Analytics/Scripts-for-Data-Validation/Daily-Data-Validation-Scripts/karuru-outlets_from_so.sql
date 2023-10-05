---------------------- KARURU - Outlets from Sales Order --------------------
with
sales_order_with_index as (
                            SELECT distinct date(created_date) as created_date,
                            outlet.name as outlet_name,
                            outlet.outlet_code,
                            so.outlet_id,
                            territory.id as territory_id,
                            market_developer.id as market_developer_id,
                            market_developer.first_name as market_developer_first_name,
                            market_developer.last_name as market_developer_last_name,
                            row_number()over(partition by outlet_id order by created_date desc) as last_territory_index,
                            row_number()over(partition by name  order by last_modified_date desc) as index
                            FROM `kyosk-prod.karuru_reports.sales_order` so
                            WHERE date(created_date) >= date_sub(current_date, interval 30 day)
                            and territory_id not in ('DKasarani', 'Kyosk HQ', 'Test KE Territory', 'Kyosk TZ HQ', 'Test UG Territory')
                            and territory.country_code = 'ke'
                            --and outlet.outlet_code = 'IJZU'
                            ),
sales_order_summary as (
                        select *except(index, last_territory_index),
                        --row_number()over(partition by )
                        from sales_order_with_index
                        where index = 1  and last_territory_index = 1
                        order by 1
                        )
select *
from sales_order_summary