---------------------- Karuru ------------------
--------------- Vehicle Assignment Mashup ----------------
with
karuru_vehicle_assignment as (
                              SELECT *,
                              row_number()over(partition by id order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.vehicle_assignment` 
                              WHERE date(created_at) >= "2023-01-01"
                              ),
vehicle_assignment as (
                        select distinct id as vehicle_assignment_id,
                        vehicle_id,
                        driver_id,
                        date_assigned,
                        date_unassigned
                        from karuru_vehicle_assignment
                        where index = 1
                        ),
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
                        WHERE date(created_at) >='2022-01-01'
                        ),
vehicle_type as (
                  select distinct id,
                  car_type,
                  vehicle_capacity 
                  from karuru_vehicle_type
                  where index = 1
                  ),
vehicle_assignment_mashup as (
                              select va.*,
                              v.license_plate,
                              v.code,
                              vt.car_type,
                              vt.vehicle_capacity
                              from vehicle_assignment va
                              left join vehicles v on va.vehicle_id = v.id
                              left join vehicle_type vt on v.vehicle_type_id = vt.id
                              where date_unassigned is null
                              order by date_assigned desc, 1
                              )
select * from vehicle_assignment_mashup