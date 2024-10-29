------------- Vehicles ------------
with
vehicle as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle` 
            --WHERE territory_id not in ("Kyosk HQ","Kyosk TZ HQ","Test FC","Test Fresh TZ Territory","Test KE Territory","Test NG Territory","Test TZ Territory","Test UG Territory","Test254") 
            where date(created_at) >= '2023-10-01'
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
              case when type ='' then null else type end as type,
              case when driver_id = '' then null else driver_id end as driver_id,
              case when service_provider_id = '' then null else service_provider_id end as service_provider_id,
              type,
              case when volume = '' then null else volume end as volume,
              case when load_capacity = '' then null else load_capacity end as load_capacity,
              --make,
              --models,
              --kyosk_acquisition_date,
              --fuel_type,
              --color,
              --odometer_reading,
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              )
              
select *
--max(created_at) as max_created_at, max(updated_at) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
from vehicle_cte
--where date(updated_at) = current_date
--where license_plate like "K%"
--where license_plate = 'KMFK 690K'