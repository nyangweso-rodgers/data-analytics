-------------- Karuru ---------------
-------------- DTs ---------------------
with
karuru_dts as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                --where date(created_at) >= '2023-08-07'
                --where date(created_at) <= '2023-11-08'
                where date(created_at) between '2023-08-01' and '2024-01-23'
                and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and is_pre_karuru = false
              ),
dts_summary as (
                select distinct date(created_at) as created_at,
                country_code,
                territory_id,
                id,
                code,
                status,
                driver.id as driver_id,
                driver.code as driver_code,
                driver.name as driver_name,
                vehicle_id,
                service_provider.id as service_provider_id,
                service_provider.name as service_provider_name
                from karuru_dts
                where index = 1
                and status not in ('CANCELLED')
                and country_code = 'TZ'
                order by territory_id, created_at desc
              ),
--------------- Vehicles -----------------
karuru_vehicle as (
                    SELECT *,
                    row_number()over(partition by id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.vehicle` 
                    WHERE date(created_at) >= '2023-10-01'
                    ),
vehicles as (
              select distinct
              id,
              license_plate,
              code,
              vehicle_type_id
              from karuru_vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
karuru_vehicle_type as (  
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.vehicle_type` 
                        WHERE date(created_at) >='2023-10-13'
                        ),
vehicle_type as (
                select distinct id,
                car_type,
                vehicle_capacity 
                from karuru_vehicle_type
                where index = 1
                ),
------------------ Report ---------------
dts_with_vehicles as (
                      select dts.*,
                      v.license_plate,
                      v.code as vehicle_code,
                      v.vehicle_type_id,
                      vt.car_type,
                      vt.vehicle_capacity
                      from dts_summary dts
                      left join vehicles v on dts.vehicle_id = v.id
                      left join vehicle_type vt on v.vehicle_type_id = vt.id
                      order by territory_id, created_at desc
                      ),
dts_with_timestamps as (
                        select distinct date(created_at) as created_at,
                        date(status_change_history.change_time) as completed_date,
                        country_code,
                        territory_id,
                        id,
                        code,
                        status,
                        from karuru_dts, unnest(status_change_history) status_change_history
                        where index = 1
                        and status_change_history.to_status = 'COMPLETED'
                        ORDER BY 1 DESC
                      )
select*
from dts_with_timestamps
--where id = '0EDDC4PK2ZW49'