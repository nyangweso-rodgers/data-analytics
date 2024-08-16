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
                          select distinct created_at,
                          updated_at,
                          bq_upload_time,
                          country_code,
                          company_code,
                          id,
                          name,
                          owner,
                          provider_type,
                          disabled,
                          is_transporter,
                          supplier_group,
                          from service_provider
                          where index = 1
                          ),
-------------------- Mashup -------------------------------
vehicles_with_service_provider as (
                                  select v.*,
                                  sp.name as vehicle_service_provider_name,
                                  sp.country_code
                                  from vehicle_cte v
                                  left join service_provider_cte sp on v.service_provider_id = sp.id
                                  )
              
select *
--max(created_at) as max_created_at, max(updated_at) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
from vehicles_with_service_provider
--where date(updated_at) = current_date
--where license_plate like "K%"
where license_plate = 'KMFK 690K'