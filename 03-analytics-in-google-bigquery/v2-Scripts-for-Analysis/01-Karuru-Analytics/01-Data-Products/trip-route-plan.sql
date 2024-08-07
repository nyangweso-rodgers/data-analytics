--------------------- trip route plan ----------------------
with
trip_route_plan as (
                    SELECT *,
                    row_number()over(partition by trip_id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.trip_route_plan` 
                    WHERE date(created_at) > "2021-07-26"
                    ),
trip_route_plan_cte as (
                        select distinct created_at,
                        updated_at,
                        bq_upload_time,
                        trip_id
                        from trip_route_plan
                        where index = 1
                        )
select count(distinct trip_id)
--max(created_at) as max_created_at_datetime, max(updated_at) as max_updated_at_datetime, max(bq_upload_time) as max_bq_upload_time_datetime
from trip_route_plan_cte