----- Karuru ------------
--------- Vehicle Type ----------------
with
karuru_vehicle_type as (  
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.vehicle_type` 
                        WHERE date(created_at) >='2023-10-13'
                        ),
vehicle_type as (
                select distinct id,
                date(created_at) as created_at,
                car_type,
                vehicle_capacity 
                from karuru_vehicle_type
                where index = 1
                )
select min(created_at)
from vehicle_type