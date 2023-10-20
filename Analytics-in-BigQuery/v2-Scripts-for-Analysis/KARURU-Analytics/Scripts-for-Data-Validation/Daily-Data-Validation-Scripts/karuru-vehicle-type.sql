-------------- Karuru ----------
--------- Vehicle Type ---------------
with
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
                  and id = '0D6GETFGEDDKY'
                  )
select * from vehicle_type