----------- SO & DN Items --------------------
with
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where date(created_date) = '2024-06-01' 
                --where date(created_date) >= date_sub(current_date, interval 1 month)
                --and is_pre_karuru = false
                where date(created_date) between '2024-06-01' and '2024-06-30'
                ),
sales_order_item as (
                    select distinct  created_date,
                    --extract( day from date(created_date)) as get_day,
                    --format_date('%A', date(created_date)) as get_sale_order_day,
                    so.id,
                    so.name,
                    so.created_on_app,
                    so.order_status,
                    so.territory.country_code as country_code,
                    so.territory_id,
                    so.route_id,
                    --so.route_name,
                    so.market_developer.id as market_developer_id,
                    so.market_developer_name,
                    --outlet.latitude,
                    --outlet.longitude,
                    so.outlet_id,
                    i.product_bundle_id,
                    i.uom,
                    --i.fulfilment_status
                    --sum(i.total) as ordered_amount
                    from sales_order so, unnest(items) i
                    where index = 1
                    --and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                    --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                    --group by 1,2,3,4,5
                    ),
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where date(created_at)  > '2024-06-01' 
                --where date(created_at) >= date_sub(current_date, interval 1 month)
                --where date(created_at) between '2024-06-01' and '2024-06-30'
                ),
delivery_notes_items as (
                        select distinct created_at,
                        --dn.country_code,
                        --dn.territory_id,
                        --dn.route_id,
                        dn.route_name,
                        id,
                        code,
                        dn.sale_order_id,
                        row_number()over(partition by dn.sale_order_id order by dn.created_at desc) as rescheduled_dn_index,
                        dn.status,
                        i.product_bundle_id,
                        i.uom,
                        i.status as item_status,
                        i.total_orderd,
                        i.total_delivered,
                        --date(dn.delivery_date) as delivery_date,
                        --sum(total_delivered) as total_delivered
                        from delivery_notes dn, unnest(order_items) i
                        where index = 1
                        --and country_code = 'NG'
                        --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                        --and dni.status = 'ITEM_FULFILLED'
                        --group by 1,2,3,4,5
                        ),
sales_order_and_delivery_notes_mashup as (
                                          select distinct --date(soi.created_date) as sales_order_creation_date,
                                          --date(dni.created_at) as delivery_note_creation_date,
                                          soi.created_date as sales_order_creation_datetime,
                                          dni.created_at as delivery_note_creation_datetime,

                                          soi.country_code,
                                          soi.territory_id,
                                          soi.route_id,
                                          dni.route_name,
                                          soi.market_developer_id,
                                          soi.market_developer_name,
                                          soi.outlet_id,
                                          soi.created_on_app,
                                          soi.id as sale_order_id,
                                          dni.id as delvery_note_id,
                                          soi.name as sale_order_code,
                                          dni.code as delivery_note_code,
                                          soi.order_status as sales_order_status,
                                          dni.status as delivery_note_status,
                                          soi.product_bundle_id,
                                          soi.uom,
                                          dni.total_orderd,
                                          dni.total_delivered,
                                          --count(distinct soi.order_date) as count_order_dates,
                                          --count(distinct soi.id) as count_sale_orders,
                                          --count(distinct soi.get_sale_order_day) as count_sale_order_week_days
                                          from sales_order_item soi
                                          left join delivery_notes_items dni on soi.id = dni.sale_order_id and soi.product_bundle_id = dni.product_bundle_id
                                          where rescheduled_dn_index =1
                                          --group by 1,2
                                          ),
route_analysis as (
                  select distinct country_code,
                  territory_id,
                  route_id,
                  route_name,
                  count(distinct market_developer_id) as count_of_marker_developers,
                  count(distinct sale_order_id) as count_of_sale_orders,
                  count(distinct delvery_note_id) as count_of_dns,
                  count(distinct (case when created_on_app = 'Duka App' then sale_order_id else null end)) as duka_app_orders,
                  count(distinct (case when created_on_app = 'AgentApp' then sale_order_id else null end)) as agent_app_orders,
                  count(distinct(case when delivery_note_status = 'EXPIRED' then sale_order_id else null end)) as expired_dns,
                  count(distinct(case when delivery_note_status IN ('PAID') then sale_order_id else null end)) as paid_dns,
                  count(distinct(case when delivery_note_status IN ('DRIVER_CANCELLED', 'OPS_CANCELLED', 'USER_CANCELLED') then sale_order_id else null end)) as cancelled_dns,
                  count(distinct product_bundle_id) as count_of_skus,
                  sum(total_orderd) as total_orderd,
                  sum(total_delivered) as total_delivered
                  from sales_order_and_delivery_notes_mashup
                  group by 1,2,3,4
                  )
select *
--distinct sales_order_id, count(distinct delvery_note_id) as delvery_note_id
--distinct sales_order_status, delivery_note_status
--from sales_order_and_delivery_notes_mashup
from route_analysis
--from delivery_notes_items
--from sales_order_item
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
and country_code = 'ke'
--and sale_order_id = 'SO-0G76VRKHC05AK'
order by 1,2,3