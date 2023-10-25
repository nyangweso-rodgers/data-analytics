-------------- Karuru ---------------
-------------- DTs ---------------------
with
karuru_dts as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                where date(created_at) >= '2023-07-01'
                and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory')
                and is_pre_karuru = false
                and country_code = 'KE'
              ),
dts_summary as (
                select distinct date(created_at) as created_at,
                date(completed_time) as completed_time,
                country_code,
                territory_id,
                id as delivery_trip_id,
                code,
                status,
                --status as delivery_trip_status,
                driver.id as driver_id,
                --driver.code as driver_code,
                --driver.name as driver_name,
                --vehicle.id as vehicle_id,
                from karuru_dts
                where index = 1
                and status = 'COMPLETED'
                --and id = '0DWMJ4A3Y3NB3'
               -- and id = '0DWKXHDDMJWEX'
              )
select distinct completed_time
from dts_summary