with    
------------------------ Date Variabes ------------------------------
vars AS (
  --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE)  as current_end_date ),
  SELECT DATE '2024-10-16' as current_start_date,  DATE '2024-10-17' as current_end_date ),
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                and order_status not in ('INITIATED')
                and territory.country_code = 'ke'
                and territory_id = 'Ruiru'
                --and date(created_date) between '2024-09-01' and '2024-09-12' 
                and date(created_date) >= date_sub(current_date, interval 1 month)
                --and id = 'SO-0HKMX1VCDRVSG' # test reschedueles
                ),
so_items_cte as (
                select distinct so.created_date,

                so.last_modified_date,
                so.bq_upload_time,

                so.territory.country_code as country_code,
                so.territory_id,
                so.route.id as route_id,
                so.route.route_name as route_name,

                so.outlet_id,
                so.id,
                so.name,
                so.created_on_app,
                so.market_developer.id as market_developer_id,
                so.market_developer_name,
                so.order_status,

                i.category_id,
                i.product_bundle_id,
                i.uom,
                i.fulfilment_status,
                i.catalog_item_qty,
                i.selling_price,
                i.net_total,
                i.discount_amount,
                i.inventory_items,
                i.promotion_type,
                i.promotion_on,
                i.discount_type
                from sales_order so, unnest(items) i
                where index = 1
                ),
so_inventory_items_cte as (
                          select distinct soi.created_date,
                          soi.last_modified_date,
                          soi.bq_upload_time,

                          soi.country_code,
                          soi.territory_id,
                          ii.fulfilment_center_id,
                          ii.fulfilment_center_name,
                          soi.route_id,
                          soi.route_name,

                          soi.outlet_id,
                          soi.id,
                          soi.name,
                          soi.created_on_app,
                          soi.order_status,

                          soi.market_developer_id,
                          soi.market_developer_name,

                          
                          soi.category_id,
                          soi.product_bundle_id,
                          soi.uom,
                          soi.fulfilment_status,
                          soi.catalog_item_qty,
                          soi.selling_price,
                          soi.net_total,
                          soi.discount_amount,
                          string_agg(distinct cast(ii.conversion_factor as string), "/" order by cast(ii.conversion_factor as string)) as conversion_factor,
                          string_agg(distinct ii.stock_item_id, "/" order by ii.stock_item_id) as stock_item_id,
                          string_agg(distinct ii.stock_uom, "/" order by ii.stock_uom) as stock_uom,
                          sum(ii.inventory_item_qty) as inventory_item_qty,
                          soi.promotion_type,
                          soi.promotion_on,
                          soi.discount_type
                          from so_items_cte soi, unnest(inventory_items) ii
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,29,30,31
                          ),
------------------------------ Delivery Notes ------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > date_sub(current_date, interval 1 month)
                ),
dn_items_cte as (
                  select distinct created_at,
                  --date(delivery_date) as delivery_date,
                  --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                  --country_code,
                  --territory_id,
                  --route_id,
                  --route_name,
                  id,
                  code,
                  dn.sale_order_id,
                  --dn.sale_order_code,
                  dn.is_reschedule,
                  dn.status,
                  delivery_trip_id,
                  --payment_request_id,
                  --agent_name as market_developer,
                  --outlet.phone_number,
                  --outlet_id,
                  --outlet.name as outlet_name,
                  --outlet.outlet_code as outlet_code,
                  --outlet.latitude,
                  --outlet.longitude,
                  oi.product_bundle_id,
                  oi.uom,
                  oi.status as item_status,
                  --oi.item_group_id,
                  --sum(oi.total_orderd) as total_orderd,
                  --sum(oi.total_delivered) as  
                  oi.total_delivered,
                  oi.net_total_delivered,
                  from delivery_notes dn, unnest(order_items) oi
                  where index = 1
                  --and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                  --and oi.status = 'ITEM_FULFILLED'
                  ),
------------------------------ Delivery Trips -----------------------
--------------------------- Mashup ----------------------
so_dn_and_dt_cte as (
                    select distinct date(soii.created_date) as so_creation_date,

                    soii.country_code,
                    soii.territory_id,
                    --soii.fulfilment_center_id,
                    soii.fulfilment_center_name,

                    --soii.outlet_id,
                    soii.id as so_id,
                    soii.name as so_code,
                    soii.order_status as so_order_status,

                    soii.fulfilment_status as soi_fulfilment_status,
                    soii.product_bundle_id,
                    soii.uom,

                    sum(soii.discount_amount) as so_discount_amount,
                    sum(soii.net_total) as soi_net_total,

                    date(dni.created_at) as dn_created_at,
                    dni.code as dn_code,
                    dni.is_reschedule,
                    dni.status as dn_status,
                    dni.item_status as dni_item_status,
                    dni.delivery_trip_id,
                    sum(dni.total_delivered) as dni_total_delivered,
                    sum(dni.net_total_delivered) as dni_net_total_delivered,


                    from so_inventory_items_cte soii
                    left join dn_items_cte dni on soii.id = dni.sale_order_id and  soii.product_bundle_id = dni.product_bundle_id and soii.uom = dni.uom
                    where fulfilment_center_name = 'Khetia '
                    and is_reschedule = false
                    group by 1,2,3,4,5,6,7,8,9,10,13,14,15,16,17,18
                    order by so_creation_date desc, so_id
                    ),
so_dn_and_dt_cte_agg_cte as (
                            select distinct so_creation_date,

                            count(distinct so_id) as sales_order_count,
                            sum(soi_net_total) as sales_order_ordered_amount,
                            sum(case when dn_status in ('CASH_COLLECTED','DELIVERED', 'PAID') AND dni_item_status in ('ITEM_FULFILLED') then dni_net_total_delivered else 0 end) as delivered_amount
                            from so_dn_and_dt_cte,vars where so_creation_date between current_start_date and current_end_date
                            group by 1
                            order by so_creation_date desc
                            )
select distinct so_order_status, soi_fulfilment_status, dn_status, dni_item_status from so_dn_and_dt_cte order by 1,3
--select * from so_dn_and_dt_cte 
--select * from so_dn_and_dt_cte_agg_cte 