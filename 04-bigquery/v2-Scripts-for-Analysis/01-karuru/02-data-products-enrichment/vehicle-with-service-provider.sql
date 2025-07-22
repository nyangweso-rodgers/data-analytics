------------- Vehicles ------------
with
vehicle as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle` 
            WHERE territory_id not in ("Kyosk HQ","Kyosk TZ HQ","Test FC","Test Fresh TZ Territory","Test KE Territory","Test NG Territory","Test TZ Territory","Test UG Territory","Test254") 
            and date(created_at) >= '2023-10-01'
            ),
vehicle_cte as (
              select distinct created_at,
              updated_at,
              bq_upload_time,
              audit.created_by,
              audit.modified_by,
              territory_id,
              on_trip,
              id,
              license_plate,
              case when driver_id = '' then null else driver_id end as driver_id,
              case when service_provider_id = '' then null else service_provider_id end as service_provider_id,
              type,
              case when volume = '' then null else volume end as volume,
              case when load_capacity = '' then null else load_capacity end as load_capacity,
              make,
              --models,
              --kyosk_acquisition_date,
              --fuel_type,
              color,
              --odometer_reading,
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
---------------------------- Service Provider --------------------------
service_provider as (
                      SELECT *,
                      row_number()over(partition by id order by updated_at desc) as index
                      FROM `kyosk-prod.karuru_reports.service_provider` 
                      WHERE date(created_at) > "2021-01-01"
                      
                      ),
service_provider_cte as (
                          select distinct --created_at,
                          --updated_at,
                          --bq_upload_time,
                          --country_code,
                          --company_code,
                          id,
                          name,
                          owner,
                          provider_type,
                          disabled,
                          --is_transporter,
                          --supplier_group,
                          from service_provider
                          where index = 1
                          ),
--------------------- Delivery Trip ------------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date(created_at) = current_date
                where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 5 month)
                --where date(created_at) between '2023-08-01' and '2024-01-23'
                and status in ('DISPATCHED', 'COMPLETED', 'DISPATCHING')
              ),
delivery_trips_cte as (
                          select distinct date(created_at) as created_at_date,
                          created_at as created_at_datetime,
                          country_code,
                          territory_id,
                          id,
                          code,
                          --status,
                          --vehicle.id as vehicle_id,
                          --delivery_note_ids as delivery_note_id,
                          --driver.id as driver_id,
                          driver.name as driver_name,
                          --dt.driver_provider_id,
                          vehicle_id,
                          /*vehicle.licence_plate,
                          vehicle.vehicle_type,
                          vehicle_v2.id as vehicle_v2_id,
                          vehicle_v2.license_plate as vehicle_v2_license_plate,
                          vehicle_v2.type as vehicle_v2_type,
                          vehicle_v2.load_capacity as vehicle_v2_load_capacity,
                          vehicle_v2.volume as vehicle_v2_volume,*/
                          dt.vehicle_provider_id,
                          from delivery_trips dt
                          where index = 1
                          
                        ),
-------------------- Mashup -------------------------------
vehicle_with_latest_deliveries as (
                                select distinct v.created_at,
                                v.updated_at,
                                v.bq_upload_time,
                                v.created_by,
                                v.modified_by,
                                v.on_trip,
                                v.id,
                                v.license_plate,
                                v.type,
                                v.load_capacity,
                                sp.name as vehicle_service_provider_name,
                                last_value(dt.created_at_date)over(partition by dt.vehicle_id order by dt.created_at_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_delivery_trip_date,
                                last_value(dt.country_code)over(partition by dt.vehicle_id order by dt.created_at_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_country_code,
                                last_value(dt.territory_id)over(partition by dt.vehicle_id order by dt.created_at_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_territory_id,
                                last_value(dt.driver_name)over(partition by dt.vehicle_id order by dt.created_at_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_driver_name,
                                from vehicle_cte v
                                left join delivery_trips_cte dt on v.id = dt.vehicle_id
                                left join service_provider_cte sp on v.service_provider_id = sp.id --and sp.id = dt.vehicle_provider_id
                                )
select *
--max(created_at) as max_created_at, max(updated_at) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
from vehicle_with_latest_deliveries
where latest_territory_id not in ('Test KE Territory') 
and latest_country_code in ('KE')
--and id = '0FJC8H3HSW917'
--where date(updated_at) = current_date
--where license_plate like "K%"
--where license_plate = 'KMFK 690K'