----------- SO & DN Items --------------------
with
------------------------------- Sales Order ----------------------
/*
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                --and territory.country_code = 'ke'
                where territory_id = 'Ruiru'
                --where date(created_date) = '2024-06-01' 
                and date(created_date) >= date_sub(current_date, interval 1 week)
                ),
sales_order_item_cte as (
                    select distinct  created_date,
                    --extract( day from date(created_date)) as get_day,
                    --format_date('%A', date(created_date)) as get_sale_order_day, 
                    so.territory.country_code as country_code,
                    so.territory_id,
                    so.route.id as route_id,
                    so.route.route_name as route_name,

                    so.id,
                    so.name,
                    so.created_on_app,
                    so.order_status,

                    --so.market_developer.id as market_developer_id,
                    --so.market_developer_name,

                    so.outlet_id,
                    cast(outlet.latitude as float64) as latitude,
                    cast(outlet.longitude as float64) as longitude,

                    i.category_id,
                    i.product_bundle_id,
                    i.uom,
                    i.fulfilment_status,
                    i.catalog_item_qty,
                    i.net_total
                    from sales_order so, unnest(items) i
                    where index = 1
                    --and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                    --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                    --group by 1,2,3,4,5
                    ),
*/
--------------------------- Scheduled Query - Sales Order Item -----------------------------------
scheduled_sales_order_item_cte as (
                                  SELECT distinct date(created_date) as created_date,
                                  delivery_window_id,
                                  deliveey_window_start_time,
                                  deliveey_window_end_time,
                                  scheduled_delivery_date,
                                  country_code, 
                                  territory_id,
                                  route_id,
                                  route_name,
                                  outlet_id,
                                  latitude,
                                  longitude,

                                  id,
                                  name,
                                  created_on_app,
                                  order_status,
                                  market_developer_id,
                                  market_developer_name,

                                  category_id,
                                  item_group_type,
                                  product_bundle_id,
                                  uom,
                                  fulfilment_status,
                                  catalog_item_qty,
                                  selling_price,
                                  net_total
                                  FROM `kyosk-prod.karuru_test.sales_order_items` 
                                  ),
------------------- Sales Order Cancellations ----------------------
/*
sales_order_cancellatios_cte as (
                            select distinct territory_id,
                            outlet_id, 
                            count(distinct id) as sale_orders_count,
                            count(distinct(case when order_status in ('USER_CANCELLED', 'OPS_CANCELLED', 'CANCELLED', 'EXPIRED') then id else null end)) as cancelled_orders_count,
                            round(count(distinct(case when order_status in ('USER_CANCELLED', 'OPS_CANCELLED', 'CANCELLED', 'EXPIRED') then id else null end)) / count(distinct id),2) as cancelled_orders_percent
                            --count(distinct(case when order_status in ('USER_CANCELLED') then id else null end)) as users_cancelled_orders_count,
                            --count(distinct(case when order_status in ('OPS_CANCELLED') then id else null end)) as ops_cancelled_orders_count,
                            --count(distinct(case when order_status in ('CANCELLED') then id else null end)) as cancelled_orders_count,
                            --count(distinct(case when order_status in ('EXPIRED') then id else null end)) as expired_orders_count
                            from sales_order_item_cte
                            group by 1,2
                            ),
outlets_cancellation_risk_segment as (
                            select *,
                            case
                              when (cancelled_orders_percent <= 0.3) then 'LOW'
                              when (cancelled_orders_percent <= 0.6) then 'MEDIUM'
                              when (cancelled_orders_percent <= 1) then 'HIGH'
                            else 'UNSET' end as order_cancellation_risk
                            from sales_order_cancellatios_cte
                            ),
*/
------------------------- Upload - Customer Sales Order Cancellations Risks ---------------------------------
uploaded_outlets_cancellations_risks as (
                                          SELECT distinct territory_id, 
                                          outlet_id, 
                                          order_cancellation_risk 
                                          FROM `kyosk-prod.karuru_test.sales_order_cancellations_risks` 
                                          ),
--------------------------------- Upload - Item Group Type ------------------------
item_group_mapping as (
                        SELECT distinct country_code,
                        item_group_id,
                        type
                        FROM `kyosk-prod.karuru_upload_tables.item_group_mapping` 
                        where country_code = 'KE'
                        ),
----------------------------------- Delivery Notes ---------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at)  > '2024-09-11' 
                where date(created_at) >= date_sub(current_date, interval 1 week)
                ),
delivery_notes_items as (
                        select distinct created_at,
                        coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                        --dn.country_code,
                        --dn.territory_id,
                        --dn.route_id,
                        dn.route_name,
                        id,
                        code,
                        dn.sale_order_id,
                        row_number()over(partition by dn.sale_order_id order by dn.created_at desc) as rescheduled_dn_index,
                        dn.delivery_trip_id,
                        dn.status,
                        i.product_bundle_id,
                        i.uom,
                        i.status as item_status,
                        i.total_orderd,
                        i.net_total_ordered,
                        --i.total_delivered,
                        case
                          when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                          and i.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then i.total_orderd - (i.qty_delivered * i.discount_amount)
                        else 0 end as delivery_note_dispatched_amount,
                        case 
                          when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and i.status in ('ITEM_FULFILLED') then i.net_total_delivered 
                        else 0 end as gmv_vat_incl,
                        --sum(total_delivered) as total_delivered
                        from delivery_notes dn, unnest(order_items) i
                        where index = 1
                        --and country_code = 'NG'
                        --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                        --and dni.status = 'ITEM_FULFILLED'
                        --group by 1,2,3,4,5
                        ),
--------------------- Delivery Trips --------------------------------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date(created_at) = current_date
                where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 week)
                --where date(created_at) between '2024-07-01' and '2024-07-15'
                --and is_pre_karuru = false
              ),
delivery_trips_cte as (
                      select distinct --date(created_at) as created_at,
                      created_at,
                      --updated_at,
                      --bq_upload_time,
                      --country_code,
                      --territory_id,
                      id,
                      code,
                      status,
                      vehicle.id as vehicle_id,
                      vehicle.licence_plate,
                      vehicle.vehicle_type,
                      delivery_note_ids as delivery_note_id,

                      vehicle_id,
                      vehicle_v2.id as vehicle_v2_vehicle_id,
                      vehicle_v2.license_plate as vehicle_v2_license_plate,
                      vehicle_v2.type as vehicle_v2_type,
                      case when vehicle_v2.load_capacity = '' then null else vehicle_v2.load_capacity end as vehicle_v2_load_capacity,
                      --driver.id as driver_id,
                      --driver.code as driver_code,
                      --driver.name as driver_name,
                      --vehicle_id,
                      --service_provider.id as service_provider_id,
                      --service_provider.name as service_provider_name
                      
                      from delivery_trips, unnest(delivery_note_ids) delivery_note_ids
                      where index = 1
                    ),
-------------------------------------- Vehicle ----------------
vehicle as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle` 
            WHERE date(created_at) >= '2023-10-01'
            ),
vehicle_cte as (
              select distinct --updated_at,
              id,
              license_plate,
              case when type ='' then null else type end as type,
              case when load_capacity = '' then null else load_capacity end as load_capacity,
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
------------------- Fulfiment Center ----------------
fulfillment_center as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.fulfillment_center` 
                        WHERE date(created_at) > "2021-06-27" #start date
                        ),
fulfillment_center_cte as (
                            select distinct --date(created_at) created_at,
                            id,
                            name,
                            --country_code,
                            cast(location.latitude as float64) as latitude,
                            cast(location.longitude as float64) as longitude
                            from fulfillment_center
                            where index =1 
                            ),
----------- front margins scheduled query ----------------
front_margins_cte as (
                        SELECT distinct creation_date,
                        delivery_date,
                        company,
                        territory_id,
                        --route_id,
                        --route_name
                        sales_invoice,
                        kyosk_delivery_note as delivery_note_id,
                        delivery_note,
                        --delivery_trip_id,
                        --delivery_trip_code,
                        --outlet_id,

                        --item_group_id,
                        item_code,
                        uom,
                        item_name_of_packed_item,
                        uom_of_packed_item,
                        tax_rate,
                        qty_of_packed_item,
                        incoming_rate,
                        total_incoming_rate,
                        base_amount,
                        base_net_amount
                        FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                        wHERE delivery_date > date_sub(date_trunc(current_date, month), interval 1 month)
                        --where delivery_date = date_sub(current_date, interval 1 day)
                        --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                        ),
catalog_items_front_margins_cte as (
                                select distinct creation_date,
                                company,
                                territory_id,
                                delivery_note_id,
                                sales_invoice,
                                item_code,
                                uom,
                                tax_rate,
                                round(sum(base_amount),0) as gmv_vat_incl,
                                round(sum(base_net_amount),0) as gmv_vat_excl,
                                round(sum(total_incoming_rate),0) as avg_cost_vat_excl,
                                round(sum(total_incoming_rate) /sum(qty_of_packed_item),1) as unit_avg_cost_vat_excl,
                                round(sum(base_net_amount) - sum(total_incoming_rate),2) as front_margin_vat_excl,
                                from front_margins_cte
                                group by 1,2,3,4,5,6,7,8
                                ),
latest_catalog_front_margins_cte as ( 
    select distinct territory_id,
    item_code,
    uom,
    LAST_VALUE(tax_rate) OVER (PARTITION BY territory_id, item_code, uom ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_tax_rate,
    LAST_VALUE(unit_avg_cost_vat_excl) OVER (PARTITION BY company, item_code, uom ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_country_unit_avg_cost_vat_excl,
    LAST_VALUE(unit_avg_cost_vat_excl) OVER (PARTITION BY territory_id, item_code, uom ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_territory_unit_avg_cost_vat_excl
    from catalog_items_front_margins_cte
    ),
------------------- Orders & Deliveries Mashup --------------------
orders_with_deliveries_cte as (
              select distinct date(soi.created_date) as so_creation_date,
              --date(dni.created_at) as delivery_note_creation_date,
              --soi.created_date as sales_order_creation_datetime,
              --dni.created_at as delivery_note_creation_datetime,
              soi.country_code,
              soi.territory_id,
              soi.route_id,
              dni.route_name,
              --soi.market_developer_id,
              --soi.market_developer_name,
              soi.outlet_id,
              soi.latitude as sales_order_outlet_latitude,
              soi.longitude as sales_order_outlet_longitude,
              fc.latitude as fulfilment_center_latitude,
              fc.longitude as fulfilment_center_longitude,
              round(st_distance(ST_GEOGPOINT(fc.longitude, fc.latitude), ST_GEOGPOINT(soi.longitude, soi.latitude)) / 1000,2) as sales_order_outlet_distance,

              --soi.created_on_app,
              soi.id as sale_order_id,
              soi.name as sale_order_code,
              soi.order_status as sales_order_status,
              soi.fulfilment_status as sales_order_item_fulfilment_status,

              soi.category_id,
              case
                when (igm.type is not null) then igm.type
                when (igm.type is null) and (soi.category_id in ('Flour', 'Four', 'Cooking Oil & Fats', 'Sugar')) then 'Revenue Movers'
                when (igm.type is null) and (soi.category_id in ('Home Care', 'Beauty.', 'Personal Care', 'Condiment', 'Dairy', 'Beverage', 'Cereals', 'Health', 'Baby Care')) then 'Margin Movers'
                when (igm.type is null) and (soi.category_id in ('Stationary')) then 'New Category'
              else null end as item_group_type,
              soi.product_bundle_id,
              soi.uom,
              soi.catalog_item_qty as sales_order_qty,
              soi.net_total as sales_order_ordered_amount,
              dni.net_total_ordered as delivery_note_ordered_amount,
              dni.delivery_note_dispatched_amount,
              dni.gmv_vat_incl,
              coalesce(lcfm.latest_territory_unit_avg_cost_vat_excl, lcfm.latest_country_unit_avg_cost_vat_excl) as latest_unit_avg_cost_vat_excl,
              soi.catalog_item_qty * coalesce(lcfm.latest_territory_unit_avg_cost_vat_excl, lcfm.latest_country_unit_avg_cost_vat_excl) as sales_order_projected_avg_cost_vat_excl,
              lcfm.latest_tax_rate,
              round(soi.net_total / (1+ lcfm.latest_tax_rate)) as sales_order_ordered_amount_vat_excl,
              cifm.avg_cost_vat_excl,

              date(dni.created_at) as delivery_note_creation_date,
              dni.delivery_date,
              dni.id as delvery_note_id,
              dni.code as delivery_note_code,
              dni.status as delivery_note_status,
              dni.item_status as delivery_note_item_status,
              dni.delivery_trip_id,

              date(dt.created_at) as delivery_trip_creation_date,
              dt.code as delivery_trip_code,
              dt.status as delivery_trip_status,
              coalesce(dt.vehicle_v2_license_plate, v.license_plate, 'UNSET') as vehicle_license_plate,
              coalesce(v.type, dt.vehicle_v2_type, 'UNSET') as vehicle_type,
              coalesce(v.load_capacity, dt.vehicle_v2_load_capacity) as vehicle_load_capacity,

              uocr.order_cancellation_risk,

              cifm.sales_invoice,
              cifm.front_margin_vat_excl as sales_invoice_front_margin,


              --dni.total_orderd,
              --dni.total_delivered,
              --count(distinct soi.order_date) as count_order_dates,
              --count(distinct soi.id) as count_sale_orders,
              --count(distinct soi.get_sale_order_day) as count_sale_order_week_days
              --from sales_order_item_cte soi
              from scheduled_sales_order_item_cte soi
              left join item_group_mapping igm on soi.category_id = igm.item_group_id
              left join delivery_notes_items dni on soi.id = dni.sale_order_id and soi.product_bundle_id = dni.product_bundle_id and soi.uom = dni.uom
              left join delivery_trips_cte dt on dt.id = dni.delivery_trip_id and dt.delivery_note_id = dni.id
              left join vehicle_cte v on dt.vehicle_v2_vehicle_id = v.id
              left join fulfillment_center_cte fc on soi.territory_id = fc.name
              --left join outlets_cancellation_risk_segment ocrs on soi.outlet_id = ocrs.outlet_id
              left join uploaded_outlets_cancellations_risks uocr on soi.outlet_id = uocr.outlet_id
              left join catalog_items_front_margins_cte cifm on dni.id = cifm.delivery_note_id and dni.product_bundle_id = cifm.item_code and dni.uom =  cifm.uom
              left join latest_catalog_front_margins_cte lcfm on soi.territory_id = lcfm.territory_id and soi.product_bundle_id = lcfm.item_code and soi.uom = lcfm.uom 
              where rescheduled_dn_index =1
              --and date(soi.created_date) = current_date
              and date(soi.created_date) = date_sub(current_date, interval 3 day)
              order by so_creation_date, territory_id, product_bundle_id
              ),
sales_orders_with_projections_cte as (
                select *,
                round(sales_order_ordered_amount_vat_excl - sales_order_projected_avg_cost_vat_excl, 2) as sales_order_projected_front_margins_vat_excl
                from orders_with_deliveries_cte
                )
select *
from sales_orders_with_projections_cte --where delvery_note_id = '0H6VF7TDZXPQ3'
where sale_order_id = 'SO-0H7FBSGGHSKDB'