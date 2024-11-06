
--------- Vehicle Type ----------------
with
vehicle_type as (  
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index 
                  FROM `kyosk-prod.karuru_reports.vehicle_type` 
                  WHERE date(created_at) >='2022-10-13'
                  ),
vehicle_type_cte as (
                select distinct --date(created_at) as created_at,
                id,
                code,
                car_type,
                vehicle_capacity 
                from vehicle_type
                where index = 1
                )
select count(distinct id)
from vehicle_type
--where id = '0ES8RK5SY1J7X'