--------------------- trip route plan ----------------------
with
trip_route_plan as (
                    SELECT *,
                    row_number()over(partition by trip_id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.trip_route_plan` 
                    WHERE date(created_at) > "2021-07-26"
                    ),
trip_route_plan_cte as (
                        select distinct created_at as created_at_datetime,
                        updated_at as updated_at_datetime,
                        trip_id
                        from trip_route_plan
                        where index = 1
                        )
select *
from trip_route_plan_cte
order by created_at_datetime desc