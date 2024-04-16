------------------- Karuru ----------
------------- Vehicles ------------
with
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
              )
select *
from vehicles
where license_plate like "%T463AMS%"