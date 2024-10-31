
-------------- DTs and DNs ---------------------
with
--------------------------- Delivery Trips -----------------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --where date(created_at) = current_date
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 week)
                and date(created_at) between '2024-05-01' and '2024-06-30'
                --and is_pre_karuru = false
                and country_code = 'KE'
                and status not in ('CANCELLED')
              ),
delivery_trips_cte as (
                      select distinct
                      created_at,
                      case 
                        when dt.country_code in ('TZ','KE','UG') then date_add(dt.created_at, interval 3 hour)
                        when dt.country_code in ('NG') then date_add(dt.created_at, interval 2 hour)
                      else dt.created_at end as created_at_in_local,
                      --updated_at,
                      --bq_upload_time,
                      country_code,
                      territory_id,
                      id,
                      code,
                      status,
                      --vehicle.id as vehicle_id,
                      vehicle_id,
                      vehicle_v2.id as vehicle_v2_id,
                      vehicle_v2.license_plate as vehicle_v2_license_plate,
                      case when vehicle_v2.load_capacity = '' then null else vehicle_v2.load_capacity end as vehicle_v2_load_capacity,
                      case when vehicle_v2.type = '' then null else vehicle_v2.type end as vehicle_v2_type,
                        
                      vehicle.licence_plate,
                      vehicle.vehicle_type,
                      delivery_note_ids as delivery_note_id,
                      --driver.id as driver_id,
                      --driver.code as driver_code,
                      --driver.name as driver_name,
                      --vehicle_id,
                      --service_provider.id as service_provider_id,
                      --service_provider.name as service_provider_name
                      
                      from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
                      where index = 1
                    ),
----------------------------------- Delivery Notes ----------------------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and date(created_at) between '2024-04-01' and '2024-07-31'
                --date(created_date) >= date_sub(date_trunc(current_date(), month), interval 1 month)
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 week)
                --where date(created_at) > date_sub(current_date, interval 30 day)
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
                          select distinct date(created_at) as created_at,
                          coalesce(delivery_date, updated_at) as delivery_date,
                          case 
                            when dn.country_code in ('TZ','KE','UG') then date_add(delivery_date, interval 3 hour)
                            when dn.country_code in ('NG') then date_add(delivery_date, interval 2 hour)
                          else dn.delivery_date end as delivery_date_in_local,
                          dn.delivery_window.delivery_date as scheduled_delivery_date,
                          cast(dn.delivery_window.start_time as int64) as delivery_window_start_time,
                          cast(dn.delivery_window.end_time as int64) as delivery_window_end_time,
                          case
                            when (dn.delivery_window.start_time = "8") and (dn.delivery_window.end_time = "14") then '8-14 Delivery Window'
                            when (dn.delivery_window.start_time = "13") and (dn.delivery_window.end_time = "19") then '13-19 Delivery Window'
                          else 'UNSET' end as delivey_window_name,
                          dn.territory_id,
                          --route_id,
                          delivery_trip_id,
                          id,
                          code,
                          --dn.sale_order_id,
                          dn.status,
                          --payment_request_id,
                          --agent_name as market_developer,
                          --outlet.phone_number,
                          outlet_id,
                          --outlet.name as outlet_name,
                          --outlet.outlet_code as outlet_code,
                          cast(outlet.latitude as float64) as outlet_latitude,
                          cast(outlet.longitude as float64) as outlet_longitude,
                          
                          --outlet_coordinates[OFFSET(0)] as outlet_coordinates_latiude,
                          --outlet_coordinates[OFFSET(1)] as outlet_coordinates_longitude,
                          sum(case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.total_delivered else 0 end) as gmv_vat_incl,
                          from delivery_notes dn, unnest(order_items) oi
                          where index = 1
                          --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                          --and dni.status = 'ITEM_FULFILLED'
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
                          ),
-------------------------------------- Vehicle ----------------
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
              --case when driver_id = '' then null else driver_id end as driver_id,
              --case when service_provider_id = '' then null else service_provider_id end as service_provider_id,
              case when type = '' then null else type end as type,
              --case when volume = '' then null else volume end as volume,
              case when load_capacity = '' then null else load_capacity end as load_capacity,
              from vehicle
              where index = 1
              ),
------------------------- Trip Route Plan -----------------------
trip_route_plan as (
                    SELECT *,
                    row_number()over(partition by trip_id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.trip_route_plan` 
                    WHERE date(created_at) > "2021-05-26"
                    ),
trip_route_plan_cte as (
                        select distinct created_at,
                        updated_at,
                        bq_upload_time,
                        country_code,
                        trip_id,
                        warehouse_location.outlet_location.latitude as outlet_location_latitude,
                        warehouse_location.outlet_location.longitude as outlet_location_longitude,
                        estimated_distance,
                        distance_covered_by_driver,
                        estimated_duration,
                        duration_covered_by_driver,
                        from trip_route_plan trp
                        where index = 1
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
                            country_code,
                            cast(location.latitude as float64) as latitude,
                            cast(location.longitude as float64) as longitude
                            from fulfillment_center
                            where index =1 
                            ),
--------------------- Delivery Trip, Delivery Notes, Vehicles --------------------------------
delivery_trip_and_delivery_notes_cte as (
                                            select distinct date(dt.created_at) as delivery_trip_creation_date,
                                            dt.created_at_in_local,
                                            EXTRACT(HOUR FROM dt.created_at_in_local) as delivery_trip_creation_hour,
                                            case
                                              when (EXTRACT(HOUR FROM dt.created_at_in_local) between 6 and 11) then "6-11 Trip Creation Window"
                                              when (EXTRACT(HOUR FROM dt.created_at_in_local) between 12 and 19) then "12-19 Trip Creation Window"
                                              when (EXTRACT(HOUR FROM dt.created_at_in_local) > 19) then "After 19 Trip Creation Window"
                                            else 'UNSET' end as delivery_trip_creation_window_name,
                                            dt.country_code,
                                            dt.territory_id,
                                            dt.id as delivery_trip_id,
                                            dt.code as delivery_trip_code,
                                            dt.status as delivery_trip_status,

                                            dt.vehicle_id,
                                            coalesce(dt.vehicle_v2_license_plate, v.license_plate) as vehicle_license_plate,
                                            safe_cast(coalesce(v.load_capacity, dt.vehicle_v2_load_capacity, 'UNSET') as float64) as vehicle_load_capacity,
                                            coalesce(v.type, dt.vehicle_v2_type, 'UNSET') as vehicle_type,

                                            dt.delivery_note_id,
                                            date(dn.created_at) as delivery_note_creatio_date,
                                            dn.code as delivery_note_code,
                                            dn.status as delivery_note_status,
                                            dn.scheduled_delivery_date as delivery_note_scheduled_delvery_date,
                                            dn.delivey_window_name,
                                            dn.delivery_window_start_time,
                                            delivery_window_end_time,
                                            dn.delivery_date as delivery_note_delivery_date,
                                            row_number()over(partition by dt.id order by dn.delivery_date asc) as delivery_note_delivery_asc_index,
                                            row_number()over(partition by dt.id order by dn.delivery_date desc) as delivery_note_delivery_desc_index,
                                            dn.delivery_date_in_local as delivery_note_delivery_date_in_local,
                                            EXTRACT(HOUR FROM dn.delivery_date_in_local) as delivery_hour,
                                            dn.outlet_id,
                                            dn.outlet_latitude,
                                            dn.outlet_longitude,
                                            round(st_distance(ST_GEOGPOINT(fc.longitude, fc.latitude), ST_GEOGPOINT(dn.outlet_longitude, dn.outlet_latitude)) / 1000,2) as outlet_registration_distance,
                                            --dn.outlet_coordinates_latiude,
                                            --dn.outlet_coordinates_longitude
                                            --dn.status as dn_status,
                                            --v.license_plate
                                            dn.gmv_vat_incl,
                                            trp.distance_covered_by_driver,
                                            trp.duration_covered_by_driver
                                            from delivery_trips_cte dt
                                            left join trip_route_plan_cte trp on dt.id = trp.trip_id
                                            left join delivery_notes_cte dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
                                            left join fulfillment_center_cte fc on dn.territory_id = fc.name
                                            left join vehicle_cte v on dt.vehicle_id = v.id
                                            --left join vehicle_cte v on dt.vehicle_id = v.id
                                            where dn.status not in ('RESCHEDULED', 'DRIVER_CANCELLED')
                                            order by delivery_trip_id, delivery_note_delivery_asc_index
                                            ),
updated_delivery_trip_and_delivery_notes_cte as (
                select distinct country_code,
                delivery_trip_creation_date,
                delivery_trip_creation_hour,
                delivery_trip_creation_window_name,
                delivery_trip_id,
                delivery_trip_status,
                delivery_note_id,
                delivery_note_status,
                delivery_hour,
                gmv_vat_incl,
                case
                  when (delivery_trip_creation_window_name = '6-11 Trip Creation Window') and (delivery_hour between 6 and 11) then gmv_vat_incl
                  when (delivery_trip_creation_window_name = '12-19 Trip Creation Window') and  (delivery_hour between 12 and 19) then gmv_vat_incl
                else 0 end as on_time_gmv,
                case
                  when (delivery_trip_creation_window_name = '6-11 Trip Creation Window') and (delivery_hour not between 6 and 11) then gmv_vat_incl
                  when (delivery_trip_creation_window_name = '12-19 Trip Creation Window') and  (delivery_hour not between 12 and 19) then gmv_vat_incl
                else 0 end as early_or_late_gmv,

                vehicle_license_plate,
                vehicle_load_capacity,
                vehicle_type
                from delivery_trip_and_delivery_notes_cte
                ),
------------------------ 
delivery_trips_with_first_and_last_delivery_notes_cte as (
      select distinct dtadn.delivery_trip_creation_date,
      dtadn.country_code,
      dtadn.territory_id,
      dtadn.delivery_trip_id,
      dtadn.delivery_trip_code,
      count(distinct dtadn.outlet_id) as outlets_count,
      dtadn.distance_covered_by_driver,
      f.outlet_registration_distance as first_delivery_note_distance,
      l.outlet_registration_distance as last_delivery_note_distance,

      vehicle_license_plate,
      vehicle_load_capacity,
      vehicle_type
      from delivery_trip_and_delivery_notes_cte dtadn
      left join (select distinct delivery_trip_code, outlet_registration_distance from delivery_trip_and_delivery_notes_cte where delivery_note_delivery_asc_index = 1) f on dtadn.delivery_trip_code = f.delivery_trip_code
      left join (select distinct delivery_trip_code, outlet_registration_distance from delivery_trip_and_delivery_notes_cte where delivery_note_delivery_desc_index = 1) l on dtadn.delivery_trip_code = l.delivery_trip_code
      group by 1,2,3,4,5,7,8,9,10,11,12
      ),
--------------------------------- Weekly Reports ------------------------------------
weekly_deliveries_cte as (
                            select distinct country_code,
                            date_trunc(delivery_trip_creation_date, week) as delivery_trip_creation_week,
                            count(delivery_trip_id) as delivery_trip_count,
                            sum(gmv_vat_incl) as gmv_vat_incl,
                            sum(on_time_gmv) as on_time_gmv,
                            sum(early_or_late_gmv) as early_or_late_gmv
                            from updated_delivery_trip_and_delivery_notes_cte
                            group by 1,2
                            order by 1
                            )
--select distinct * from delivery_trip_and_delivery_notes_mashup  --order by delivery_trip_id, delivery_note_delivery_asc_index
select * from delivery_trips_with_first_and_last_delivery_notes_cte
--select distinct delivery_window_start_time, delivery_window_end_time from delivery_trip_and_delivery_notes_mashup order by 1,2
--select max(created_at), max(updated_at), max(bq_upload_time) from delivery_trip_and_delivery_notes_mashup
--select * from updated_delivery_trip_and_delivery_notes_cte order by delivery_trip_id
--select * from weekly_deliveries_cte order by delivery_trip_creation_week asc