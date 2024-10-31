------------- weekly so, dn ites ---------------------
with    
------------------------ Date Variabes ------------------------------
vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE)  as current_end_date ),
  --SELECT DATE '2024-10-01' as current_start_date,  DATE '2024-10-29' as current_end_date ),
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                --and order_status not in ('INITIATED')
                --and territory.country_code = 'ke'
                where territory_id = 'Ruiru'
                --and date(created_date) between '2024-09-01' and '2024-09-12' 
                and date(created_date) >= date_sub(current_date, interval 1 month)
                --and id = 'SO-0HKMX1VCDRVSG' # test reschedueles
                --and id = 'SO-0HNE8EXM9A0Z2'
                ),
so_items_cte as (
                select distinct so.created_date,

                --so.last_modified_date,
                --so.bq_upload_time,

                so.territory.country_code as country_code,
                so.territory_id,
                --so.route.id as route_id,
                --so.route.route_name as route_name,

                --so.outlet_id,
                so.id,
                so.name,
                --so.created_on_app,
                --so.market_developer.id as market_developer_id,
                --so.market_developer_name,
                so.order_status,

                --i.category_id,
                i.product_bundle_id,
                i.uom,
                i.fulfilment_status,
                --i.catalog_item_qty,
                --i.selling_price,
                --i.total,
                i.net_total,
                --i.discount_amount,
                i.inventory_items,
                --i.promotion_type,
                --i.promotion_on,
                --i.discount_type
                from sales_order so, unnest(items) i
                where index = 1
                ),
so_inventory_items_cte as (
                          select distinct soi.created_date,
                          --soi.last_modified_date,
                          --soi.bq_upload_time,

                          soi.country_code,
                          soi.territory_id,
                          ii.fulfilment_center_id,
                          ii.fulfilment_center_name,
                          --soi.route_id,
                          --soi.route_name,

                          --soi.outlet_id,
                          soi.id,
                          soi.name,
                          --soi.created_on_app,
                          soi.order_status,

                          --soi.market_developer_id,
                          --soi.market_developer_name,

                          
                          --soi.category_id,
                          soi.product_bundle_id,
                          soi.uom,
                          soi.fulfilment_status,
                          --soi.catalog_item_qty,
                          --soi.selling_price,
                          sum(soi.net_total) as net_total,
                          --soi.discount_amount,
                          --string_agg(distinct cast(ii.conversion_factor as string), "/" order by cast(ii.conversion_factor as string)) as conversion_factor,
                          --string_agg(distinct ii.stock_item_id, "/" order by ii.stock_item_id) as stock_item_id,
                          --string_agg(distinct ii.stock_uom, "/" order by ii.stock_uom) as stock_uom,
                          --sum(ii.inventory_item_qty) as inventory_item_qty,
                          --soi.promotion_type,
                          --soi.promotion_on,
                          --soi.discount_type
                          from so_items_cte soi, unnest(inventory_items) ii
                          group by 1,2,3,4,5,6,7,8,9,10,11 
                          ),
------------------------------ Delivery Notes ------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                where date(created_at) > date_sub(current_date, interval 1 month)
                --and fullfilment_center_id = '0HEHY3146QXKF'
                --and code = 'DN-KHETIA -EIKT-0HNE8EYHB4127'
                ),
dn_items_cte as (
                  select distinct dn.created_at,
                  --d.change_time as dispatched_datetime,
                  --oc.change_time as ops_cancelled_datetime,
                  --uc.change_time as user_cancelled_datetime,
                  coalesce(date(delivery_date), date(updated_at)) as delivery_date,

                  --country_code,
                  territory_id,
                  dn.fullfilment_center_id as fulfilment_center_id,
                  --route_id,
                  --route_name,

                  dn.id,
                  dn.code,
                  dn.sale_order_id,
                  --dn.sale_order_code,

                  dn.is_reschedule,
                  dn.status,
                  
                  --delivery_trip_id,
                  --payment_request_id,
                  --agent_name as market_developer,

                  --outlet_id,
                  --outlet.name as outlet_name,
                  --outlet.outlet_code as outlet_code,

                  --oi.item_group_id,
                  oi.product_bundle_id,
                  oi.uom,
                  oi.status as item_status,

                  --sum(oi.total_orderd) as total_orderd,
                  --sum(oi.total_delivered) as  
                  --oi.total_delivered,
                  --oi.net_total_delivered,
                  oi.net_total_ordered,
                  case when dn.status in ('USER_CANCELLED') then oi.net_total_ordered else 0 end as dn_user_cancelled_amount,
                  case when dn.status in ('OPS_CANCELLED') then oi.net_total_ordered else 0 end as dn_ops_cancelled_amount,
                  case
                    when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                    and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.total_orderd - (oi.qty_delivered * oi.discount_amount)
                  else 0 end as delivery_note_dispatched_amount,
                  case
                    when dn.status in ('RESCHEDULED')
                    and oi.status in ('ITEM_RESCHEDULED') then oi.total_orderd - (oi.qty_delivered * oi.discount_amount)
                  else 0 end as dn_rescheduled_amount,
                  case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED') and oi.status in ('ITEM_CANCELLED') then oi.total_orderd else 0 end as delivery_note_cancelled_amount,
                  case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.net_total_delivered else 0 end as gmv_vat_incl,
                  from delivery_notes dn, unnest(order_items) oi
                  where index = 1
                  ),
--------------------------- Mashup ----------------------
so_dn_and_dt_items_cte as (
                            select distinct date(soii.created_date) as so_creation_date,
                            
                            soii.country_code,
                            soii.territory_id,
                            soii.fulfilment_center_id,
                            soii.fulfilment_center_name,

                            soii.id as so_id,
                            soii.name as so_code,
                            soii.order_status as so_status,

                            soii.product_bundle_id,
                            soii.uom,
                            soii.fulfilment_status as so_item_fulfilment_status,
                            soii.net_total as so_net_total,

                            --date(dni.created_at) as dn_creation_date,
                            --date(dni.dispatched_datetime) as dn_dispatched_date,
                            date(dni.delivery_date) as dn_delivery_date,
                            dni.id as dn_id,
                            dni.code as dn_code,
                            dni.status as dn_status,
                            dni.item_status as dn_item_status,
                            dni.dn_user_cancelled_amount,
                            dni.dn_ops_cancelled_amount,
                            dni.delivery_note_dispatched_amount as dn_dispatched_amount,
                            dni.dn_rescheduled_amount,
                            dni.delivery_note_cancelled_amount as dn_cancelled_amount,
                            dni.dn_rescheduled_amount + dni.delivery_note_cancelled_amount as dn_returned_amount,
                            dni.gmv_vat_incl,


                            # validate dates
                            --date(soii.created_date) = date(dni.created_at) as check_so_and_dn_creation_date,
                            --date(dni.dispatched_datetime) = date(dni.delivery_date) as check_dn_dispatch_and_delivered_dates
                            from so_inventory_items_cte soii
                            left join dn_items_cte dni on soii.id = dni.sale_order_id and soii.product_bundle_id = dni.product_bundle_id and soii.uom = dni.uom

                            where soii.fulfilment_center_id = '0HEHY3146QXKF'
                            order by so_creation_date desc, so_id, product_bundle_id, uom
                            ),
so_dns_weekly_agg_cte as (
                          select distinct date_trunc(so_creation_date, week) as so_creation_week,
                          country_code,
                          territory_id,
                          fulfilment_center_id,
                          fulfilment_center_name,
                          count(distinct so_id) as so_count,
                          count(distinct case when dn_status in ('PAID', 'DELIVERED') then dn_id else null end) as paid_dns_count,
                          sum(so_net_total) as so_net_total,
                          sum(gmv_vat_incl) as gmv_vat_incl,
                          from so_dn_and_dt_items_cte, vars
                          where date(so_creation_date) between vars.current_start_date and vars.current_end_date
                          group by 1,2,3,4,5
                          order by so_creation_week asc
                          )
select * from so_dns_weekly_agg_cte