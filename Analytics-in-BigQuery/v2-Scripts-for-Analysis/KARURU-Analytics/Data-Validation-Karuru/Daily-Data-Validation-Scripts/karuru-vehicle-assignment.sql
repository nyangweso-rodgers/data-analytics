---------------------- Karuru ------------------
--------------- Vehicle Assignment ----------------
with
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
                        and driver_id = '0D6GEYWXJDD44'
                        )
select * from vehicle_assignment