-------------- Karuru ---------------
-------------- DTs, Vehicle Assignment ---------------------
with
karuru_dts as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                where date(created_at) = '2023-10-13'
                and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory')
                and is_pre_karuru = false
                and country_code = 'KE'
              ),
dts_summary as (
                select distinct date(created_at) as created_at,
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
                --and id = '0DWMJ4A3Y3NB3'
                and id = '0DWKXHDDMJWEX'
              ),
karuru_vehicle_assignment as (
                              SELECT *,
                              row_number()over(partition by id order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.vehicle_assignment` 
                              WHERE date(created_at) >= "2023-01-01"
                              ),
vehicle_assignment as (
                        select distinct id,
                        vehicle_id,
                        driver_id,
                        date_assigned,
                        date_unassigned
                        from karuru_vehicle_assignment
                        where index = 1
                        and date_unassigned is null
                        --and driver_id = '0D6GEYWXJDD44'
                        ),
karuru_vehicle as (
                    SELECT *,
                    row_number()over(partition by id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.vehicle` 
                    WHERE date(created_at) >= '2023-10-01'
                    ),
vehicles as (
              select distinct id,
              license_plate,
              code,
              vehicle_type_id
              from karuru_vehicle
              where index = 1
              --and id = '0D6GETFGEDDKY'
              ),
karuru_vehicle_type as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index
                        FROM `kyosk-prod.karuru_reports.vehicle_type` 
                        WHERE date(created_at) >= "2022-01-01"
                        ),
vehicle_type as (
                  select distinct id,
                  code,
                  car_type,
                  vehicle_capacity
                  from karuru_vehicle_type
                  where index = 1
                  --and id = '0D6GETFGEDDKY'
                  ),
mashup as (
            select dt.*,
            va.id as vehicle_assignment_id,
            va.vehicle_id,
            va.date_assigned,
            va.date_unassigned,
            v.license_plate,
            v.code as vehicle_code,
            vt.car_type,
            vt.vehicle_capacity
            from dts_summary dt
            left join vehicle_assignment va on dt.driver_id = va.driver_id
            left join vehicles v on va.vehicle_id = v.id
            left join vehicle_type vt on v.vehicle_type_id = vt.id
            order by delivery_trip_id, date_assigned
            )
select * from mashup