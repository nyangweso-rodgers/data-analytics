
-------------- DTs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --where date(created_at) = current_date
                --and date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                where date(created_at) between '2024-05-01' and '2024-06-31'
                --and is_pre_karuru = false
              ),
delivery_trips_cte as (
                          select distinct --date(created_at) as created_at,
                          created_at,
                          updated_at,
                          bq_upload_time,
                          country_code,
                          territory_id,
                          warehouse_location.latitude as warehouse_latitude,
                          warehouse_location.latitude as warehouse_longitude,

                          is_preplanned, 
                          dispatch_time.dispatch_window_type,

                          
                          fulfillment_center_id,
                          id,
                          code,
                          status,
                          --vehicle.id as vehicle_id,
                         
                          --delivery_note_ids as delivery_note_id,
                          driver.id as driver_id,
                          driver.code as driver_code,
                          driver.name as driver_name,
                          driver_wh_id,
                          dt.driver_provider_id,

                          vehicle_id,
                          vehicle.licence_plate,
                          vehicle.vehicle_type,
                          vehicle_v2.id as vehicle_v2_id,
                          vehicle_v2.license_plate as vehicle_v2_license_plate,
                          vehicle_v2.type as vehicle_v2_type,
                          vehicle_v2.load_capacity as vehicle_v2_load_capacity,
                          vehicle_v2.volume as vehicle_v2_volume,
                          dt.vehicle_provider_id,

                          dispatched_value,
                          service_provider.id as service_provider_id,
                          service_provider.name as service_provider_name,
                          
                          
                          
                          from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
                          where index = 1
                          
                        )
select distinct driver_provider_id
--distinct country_code, count(distinct id)
--max(created_at) as max_created_at_datetime, max(updated_at) as max_updated_at_datetime, max(bq_upload_time) as max_bq_upload_time_datetime
from delivery_trips_cte
--and status not in ('CANCELLED')
--order by vehicle_id
--group by 1 order by 2 desc