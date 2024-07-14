----------- SO --------------------
with
sales_order as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              --where date(created_date) = current_date
        
              --where date(created_date) between '2024-06-01' and '2024-06-20'
              where  date(created_date) >= date_sub(current_date, interval 1 month)
              --where date(created_date) >= '2024-01-01'
              --and is_pre_karuru = false
              ),
sales_order_cte as (
                select distinct --date(created_date) as created_date,
                created_date,
                last_modified_date,
                bq_upload_time,
                so.territory.country_id,
                so.territory_id,
                so.route_id,
                so.outlet_id,
                so.id,
                so.name,
                so.order_status,
                so.created_on_app,
                so.created_by,
                market_developer.id as market_developer_id,
                so.market_developer_name,
                --so.route_name,
                --market_developer.first_name,
                --market_developer.last_name,
                --market_developer.phone_number,
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
                --group by 1,2,3,4,5,6,7,8
                ),
get_latest_sales_order_data as (
                                select distinct outlet_id,
                                last_value(route_id)over(partition by outlet_id order by created_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_route_id,
                                last_value(market_developer_id)over(partition by outlet_id order by created_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_market_developer_id,
                                last_value(market_developer_name)over(partition by outlet_id order by created_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_market_developer_name
                                from sales_order_cte
                                )
select 
max(created_date) as max_created_date, max(last_modified_date) as max_last_modified_date, max(bq_upload_time) as max_bq_upload_time
from sales_order_cte
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
--and country_id = 'Uganda'
--and route_id = '0CW5Y2F5NETG1'
--order by territory_id, created_date desc, route_id