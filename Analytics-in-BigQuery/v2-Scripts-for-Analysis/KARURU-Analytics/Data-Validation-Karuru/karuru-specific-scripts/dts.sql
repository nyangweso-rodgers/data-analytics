-------------- Karuru ---------------
-------------- DTs ---------------------
with
karuru_dts as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                where date(created_at) >= '2023-08-07'
                --where date(created_at) <= '2023-11-08'
                and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and is_pre_karuru = false
                and country_code = 'KE'
              ),
dts_summary as (
                select distinct --date(created_at) as created_at,
                country_code,
                territory_id,
                id as delivery_trip_id,
                code,
                status,
                --status as delivery_trip_status,
                --driver.id as driver_id,
                --driver.code as driver_code,
                --driver.name as driver_name,
                --vehicle.id as vehicle_id,
                from karuru_dts
                where index = 1
                --and status not in ('COMPLETED', 'CANCELLED')
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