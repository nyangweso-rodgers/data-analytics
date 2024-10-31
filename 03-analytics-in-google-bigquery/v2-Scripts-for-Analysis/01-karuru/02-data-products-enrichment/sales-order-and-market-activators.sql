----------- sales orders, market activators, market activators assignment --------------------
with
sales_order as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
              --where date(created_date) = current_date
              --where date(created_date) between '2024-07-01' and '2024-07-31'
              --where  date(created_date) >= date_sub(current_date, interval 1 month)
              --where date(created_date) >= date_sub(date_trunc(current_date,month), interval 2 day)
              --where date(created_date) >= '2024-01-01'
              --and is_pre_karuru = false
               and date(created_date) between '2024-07-23' and '2024-08-06'
              ),
sales_order_cte as (
                select distinct --date(created_date) as created_date,
                created_date,
                last_modified_date,
                bq_upload_time,
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
                --group by 1,2,3,4,5,6,7,8
                ),
----------------------- Market Activators -------------------------
market_activators as (
                      select *,
                      row_number()over(partition by id order by bq_upload_time desc) as index
                      from `karuru_reports.market_activators`
                      where date(created_at) >= '2023-08-01'
                    ),
market_activators_cte as (
                          select distinct created_at,
                          id,
                          names,
                          email,
                          msisdn,
                          market_id,
                          active
                          from market_activators
                          where index = 1
                          ),
----------------- Mashup ----------------------------
sales_order_report as (
                        select so.*,
                        ma.names as market_activator_name,
                        ma.msisdn as market_activator_msisdn,
                        ma.email as market_activator_email,
                        case
                          when (country_id in ('Uganda', 'Nigeria')) and (created_on_app = 'Duka App') then 'Kyosk App'
                          when (country_id in ('Uganda', 'Nigeria')) and (created_on_app = 'AgentApp') and (so.created_by = ma.email) then 'Market Activator'
                        else 'Market Developer' end as created_by_Role
                        from sales_order_cte so
                        left join market_activators_cte ma on so.created_by = ma.email 
                        )
select *
--max(created_date) as max_created_date, max(last_modified_date) as max_last_modified_date, max(bq_upload_time) as max_bq_upload_time
from sales_order_report

--and market_developer_phone_number is null
--and country_id = 'Uganda'
--and route_id = '0CW5Y2F5NETG1'
--order by territory_id, created_date desc, route_id
--and route_id is null
--and market_developer_phone_number is null