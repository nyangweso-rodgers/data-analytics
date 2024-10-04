----------- sales orders --------------------
with
sales_order as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
              --where date(created_date) = current_date
              --and date(created_date) between '2024-09-01' and '2024-09-18'
              --and  date(created_date) >= date_sub(current_date, interval 1 month)
              --where date(created_date) >= date_sub(date_trunc(current_date,month), interval 2 day)
              and date(created_date) >= '2024-10-01'
              --and is_pre_karuru = false
               --and date(created_date) between '2024-07-23' and '2024-08-06'
               --and id = 'SO-0H555F015N2QN'
               --and country_id = 'Kenya'
               --and name in ('SOZKVSH2024')
               --and order_status in ('PARTIALLY_DELIVERED')
              ),
sales_order_cte as (
                select distinct --date(created_date) as created_date,
                created_date,
                format_date('%A', date(created_date)) as sales_order_created_at_day_of_week,
                last_modified_date,
                bq_upload_time,
                delivery_window.id as delivery_window_id,
                delivery_window.start_time as delivery_window_start_time,
                delivery_window.end_time as delivery_window_end_time,
                so.territory.country_id,
                so.territory_id,
                so.route_id,
                --route.id as route_id,
                route.route_code,
                route.route_name,
                so.outlet_id,
                so.id,
                so.name,
                so.order_status,
                so.cancellation_reason,
                so.created_on_app,
                so.payment_type,
                so.created_by,
                market_developer.id as market_developer_id,
                so.market_developer_name,
                --market_developer.first_name,
                --market_developer.last_name,
                market_developer.phone_number as market_developer_phone_number,
                --i.fulfilment_status,
                --sum(i.total) as ordered_amount
                from sales_order so--, unnest(items) i
                where index = 1
                --and so.order_status not in ('INITIATED', 'EXPIRED', 'USER_CANCELLED')
                --and order_status in ('SUBMITTED', 'PROCESSING', 'DELIVERED', 'DISPATCHED', 'PUBLISHED')
                --and so.territory.country_code = 'ke'
                --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                --and name in ('SOIHHVO2024')
                --and market_developer_name in ('yvonne irungu')
                ),
get_latest_sales_order_report as (
                                select distinct outlet_id,
                                last_value(route_id)over(partition by outlet_id order by created_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_route_id,
                                last_value(market_developer_id)over(partition by outlet_id order by created_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_market_developer_id,
                                last_value(market_developer_name)over(partition by outlet_id order by created_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_market_developer_name
                                from sales_order_cte
                                )/*,
outlets_summary as (
                      select distinct country_id,
                      outlet_id,
                      count(distinct id) as count_of_sale_orders,
                      count(distinct case when created_day_of_week = 'Monday' then id else null end) as mon,
                      count(distinct case when created_day_of_week = 'Tuesday' then id else null end) as tue,
                      count(distinct case when created_day_of_week = 'Wednesday' then id else null end) as wed,
                      count(distinct case when created_day_of_week = 'Thursday' then id else null end) as thurs,
                      count(distinct case when created_day_of_week = 'Friday' then id else null end) as fri,
                      count(distinct case when created_day_of_week = 'Saturday' then id else null end) as sat,
                      count(distinct case when created_day_of_week = 'Sunday' then id else null end) as sun
                      from sales_order_cte
                      group by 1,2
                      order by 3 desc
                      )*/
select --distinct  order_status
--distinct country_id, delivery_window_id, delivery_window_start_time, delivery_window_end_time
max(created_date) as max_created_date, max(last_modified_date) as max_last_modified_date, max(bq_upload_time) as max_bq_upload_time
from sales_order_cte
--and market_developer_phone_number is null
--order by territory_id, created_date desc, route_id
--and route_id is null
--and market_developer_phone_number is null