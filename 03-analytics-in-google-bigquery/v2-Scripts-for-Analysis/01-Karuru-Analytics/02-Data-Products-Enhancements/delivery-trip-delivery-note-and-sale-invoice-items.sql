
-------------- DTs and DNs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date(created_at) = current_date
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 week)
                where date(created_at) between '2024-06-01' and '2024-06-30'
                --and is_pre_karuru = false
              ),
delivery_trips_cte as (
                      select distinct date(created_at) as created_at,
                      --created_at,
                      --updated_at,
                      --bq_upload_time,
                      country_code,
                      territory_id,
                      id,
                      code,
                      status,
                      vehicle.id as vehicle_id,
                      vehicle.licence_plate,
                      vehicle.vehicle_type,
                      delivery_note_ids as delivery_note_id,
                      --driver.id as driver_id,
                      --driver.code as driver_code,
                      --driver.name as driver_name,
                      --vehicle_id,
                      --service_provider.id as service_provider_id,
                      --service_provider.name as service_provider_name
                      
                      from delivery_trips, unnest(delivery_note_ids) delivery_note_ids
                      where index = 1
                    ),
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 week)
                where date(created_at) > date_sub(current_date, interval 2 month)
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
                          select distinct date(created_at) as created_at,
                          dn.delivery_window.delivery_date as scheduled_delivery_date,
                          CAST(dn.delivery_window.start_time AS INT64) as delivery_window_start_time,
                          CAST(dn.delivery_window.end_time AS INT64) as delivery_window_end_time,
                          --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                          dn.territory_id,
                          route_id,
                          route_name,
                          delivery_trip_id,
                          id,
                          code,
                          dn.is_reschedule,
                          dn.reschedule_from_dn_id,
                          dn.sale_order_id,
                          --dn.sale_order_code,
                          dn.status,
                          --delivery_trip_id,
                          --payment_request_id,
                          --agent_name as market_developer,
                          --outlet.phone_number,
                          outlet_id,
                          outlet.name as outlet_name,
                          
                          outlet.latitude,
                          outlet.longitude,
                          --outlet_coordinates[OFFSET(0)] as outlet_coordinates_latiude,
                          --outlet_coordinates[OFFSET(1)] as outlet_coordinates_longitude,
                          oi.status as dn_item_status,
                          oi.product_bundle_id,
                          oi.uom,
                          oi.qty_delivered,
                          oi.total_orderd,
                          oi.total_delivered - (oi.discount_amount * qty_delivered) as total_delivered,
                          from delivery_notes dn, unnest(order_items) oi
                          where index = 1
                          --and country_code = 'TZ'
                          --and territory_id in ('Vingunguti')
                          --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                          --and dni.status = 'ITEM_FULFILLED'
                          ),
--------------------------------------------- Vehicle ----------------------------------------------
vehicle as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle` 
            WHERE date(created_at) >= '2023-10-01'
            ),
vehicle_cte as (
              select distinct
              id,
              license_plate,
              code,
              vehicle_type_id
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
--------------------------------------------- Vehicle Type -----------------------------
vehicle_type as (  
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index 
                  FROM `kyosk-prod.karuru_reports.vehicle_type` 
                  WHERE date(created_at) >='2023-10-13'
                  ),
vehicle_type_cte as (
                select distinct --date(created_at) as created_at,
                id,
                code,
                car_type,
                vehicle_capacity 
                from vehicle_type
                where index = 1
                ),
--------------------------------- Uploaded Vehicle Mapping -------------------
uploaded_vehicle_mapping as (
                    SELECT distinct id,
                    Vehicle,
                    Car_Type,
                    Tonnage
                    FROM `kyosk-prod.uploaded_tables.upload_vehicle_mapping` 
                    ),
---------------------- Product Bundle -----------------------
product_bundle as (
                    select *,
                    row_number()over(partition by id order by modified desc) as index
                    from `kyosk-prod.karuru_reports.product_bundle` 
                    where date(creation) >= '2020-01-01'
                    ),
product_bundle_report as (
                        select distinct id,
                        uom,
                        /*si.dimension.length as sku_length,*/
                        si.dimension.width as sku_width,
                        (si.dimension.height * si.conversion_factor) as sku_height,
                        round((si.conversion_factor * si.dimension.weight),3) as sku_weight,
                        round((si.dimension.length * si.dimension.width * si.dimension.height * si.conversion_factor),3) as volume,
                        si.dimension.weight
                        from product_bundle pb,unnest(stock_items) si
                        where index = 1
                        ),
----------------------------------------- Front Margins ------------------
front_margins_report as (
                        SELECT 
                          distinct --territory_id,
                          kyosk_delivery_note,
                          delivery_note,
                          item_code,
                          uom,
                          sum(base_net_amount) as base_net_amount,
                          sum(total_incoming_rate) as total_incoming_rate
                        FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                        wHERE delivery_date > date_sub(date_trunc(current_date, month), interval 2 month)
                        group by 1,2,3,4
                        ),
---------------------------- Fulfilment Centers --------------------
fulfillment_center as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.fulfillment_center` 
                        WHERE date(created_at) > "2021-06-27" #start date
                        ),
fulfillment_center_cte as (
                            select distinct --date(created_at) created_at,
                            --id,
                            name,
                            --country_code,
                            location.latitude,
                            location.longitude
                            from fulfillment_center
                            where index =1 
                            ),
delivery_trip_and_delivery_notes_mashup as (
                  select dt.created_at as dt_creation_date,
                  dn.scheduled_delivery_date,
                  dn.delivery_window_start_time,
                  dn.delivery_window_end_time,
                  dt.country_code,
                  dt.territory_id,
                  cast(fc.latitude as float64) as warehouse_latitude,
                  cast(fc.longitude as float64) as warehouse_longitude,
                  dn.route_id,
                  dn.route_name,
                  dn.outlet_id,
                  dn.outlet_name,
                  dn.latitude as outlet_latitude,
                  dn.longitude as outlet_longitude,
                  round(st_distance(ST_GEOGPOINT(cast(fc.longitude as float64), cast(fc.latitude as float64)), ST_GEOGPOINT(cast(dn.longitude as float64), cast(dn.latitude as float64))),0) / 1000 as distance_in_kms,
                  dt.id as dt_id,
                  dt.code as dt_code,
                  dt.status as dt_status,
                  dt.delivery_note_id,
                  dn.code as dn_code,
                  dn.sale_order_id,
                  dn.is_reschedule,
                  dn.reschedule_from_dn_id,
                  dt.vehicle_id,
                  v.license_plate,
                  vt.car_type,
                  uvm.Car_Type as uploaded_car_type,
                  uvm.Tonnage as uploaded_vehicle_tonnage,
                  dn.status as dn_status,
                  
                  dn.product_bundle_id,
                  dn.uom,
                  dn.dn_item_status,
                  dn.qty_delivered,
                  --dn.total_orderd,
                  dn.total_delivered as gmv_vat_incl,
                  fmr.base_net_amount,
                  fmr.total_incoming_rate,
                  fmr.base_net_amount - fmr.total_incoming_rate as calculate_front_margins,

                  pd.sku_weight
                  from delivery_trips_cte dt
                  left join delivery_notes_cte dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
                  left join fulfillment_center_cte fc on dn.territory_id = fc.name
                  left join product_bundle_report pd on dn.product_bundle_id = pd.id and dn.uom = pd.uom
                  left join vehicle_cte v on dt.vehicle_id = v.id
                  left join vehicle_type_cte vt on v.vehicle_type_id = vt.id
                  left join uploaded_vehicle_mapping uvm on v.license_plate = uvm.Vehicle
                  left join front_margins_report fmr on dn.code = fmr.delivery_note and dn.product_bundle_id = fmr.item_code and dn.uom = fmr.uom
                  ),
report_with_zones as (
                      select *,
                      case
                          when distance_in_kms >= 101 then 'Outer Zone'
                          when distance_in_kms > 61 then 'Middle Zone'
                          when distance_in_kms >= 0 then 'Inner Zone' 
                        else null end  as delivery_zone 
                      from delivery_trip_and_delivery_notes_mashup
                      )
select *
from report_with_zones
where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
--where territory_id in ('Ruiru')
and dt_status not in ('CANCELLED')
AND dn_status IN ('PAID','DELIVERED','CASH_COLLECTED')
and dn_item_status = 'ITEM_FULFILLED'
and country_code = 'KE'