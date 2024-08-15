
-------------- DTs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --and date(created_at) = '2024-08-07'
                and date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 month)
              ),
delivery_trips_cte as (
                      select distinct date(created_at) as delivery_trip_creation_date,
                      --created_at,
                      --updated_at,
                      --bq_upload_time,
                      country_code,
                      territory_id,
                      --warehouse_location, -- to be added
                      --driver_provider_id, -- to be added
                      --vehicle_provider_id,-- to be added
                      --is_preplanned, -- to be added
                      --dispatch_time.dispatch_window_type,
                      --driver_wh_id,
                      fulfillment_center_id,
                      id,
                      code,
                      status,

                      vehicle_id,
                      vehicle_v2.id as v2_vehicle_id,
                      vehicle_v2.license_plate as v2_license_plate,
                      vehicle_v2.type as v2_vehicle_type,
                      case
                        when vehicle_v2.load_capacity = '' then null
                      else vehicle_v2.load_capacity end as v2_load_capacity,
                      case
                        when vehicle_v2.volume = '' then null
                      else vehicle_v2.volume end as v2_volume,
                      dt.vehicle_provider_id,
                      --vehicle.id as vehicle_id,
                      --vehicle.licence_plate,
                      --vehicle.vehicle_type,
                      vehicle.service_provider_id as vehicle_service_provider_id,
                      --delivery_note_ids as delivery_note_id,

                      driver.id as driver_id,
                      driver.code as driver_code,
                      driver.name as driver_name,
                      dt.driver_provider_id,
                      --driver.service_provider_id as driver_service_provider_id,
                      --service_provider.id as service_provider_id,
                      --service_provider.name as service_provider_name,
                      from delivery_trips dt--, unnest(delivery_note_ids) delivery_note_ids
                      where index = 1
                      --and status not in ('CANCELLED')
                    ),
-------------------------------------- Vehicle ----------------
vehicle as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle` 
            WHERE date(created_at) >= '2023-10-01'
            ),
vehicle_cte as (
              select distinct updated_at,
              id,
              license_plate,
              code,
              vehicle_type_id
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),

----------- Service Providers ----------
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
-------------------------------- Mashup --------------------
mashup as (
            select dt.delivery_trip_creation_date,
            dt.country_code,
            dt.territory_id,
            dt.fulfillment_center_id,
            dt.id as delivery_trip_id,
            dt.code as delivery_trip_code,
            dt.status as delivery_trip_status,
            dt.vehicle_id,
            dt.v2_vehicle_id,
            v.license_plate,
            dt.v2_license_plate,
            dt.v2_vehicle_type,
            dt.v2_load_capacity,
            dt.v2_volume,
            dt.vehicle_provider_id,
            vsp.name as vehicle_service_provider_name,

            dt.driver_id,
            dt.driver_code,
            dt.driver_name,
            dt.driver_provider_id,
            dsp.name as driver_service_provider_name
            --max(created_at), max(updated_at), max(bq_upload_time)
            from delivery_trips_cte dt
            left join vehicle_cte v on dt.vehicle_id = v.id
            left join service_provider_cte vsp on dt.vehicle_provider_id = vsp.id
            left join service_provider_cte dsp on dt.driver_provider_id = dsp.id
            )
select *
from mashup
--where vehicle_id is null
where delivery_trip_id = '0GKSHCD6NRPVH'
--and vehicle_id = '0D6GERDHYDDMR'
--where vehicle_id = '0ECQG3X288SCM'
--and id = '0FWMQR0X6QQQT'
--and delivery_note_id = '0FWK97MDPQNST'
--order by vehicle_id