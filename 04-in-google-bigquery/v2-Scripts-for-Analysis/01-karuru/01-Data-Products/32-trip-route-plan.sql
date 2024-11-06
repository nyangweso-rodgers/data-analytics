--------------------- trip route plan ----------------------
with
trip_route_plan as (
                    SELECT *,
                    row_number()over(partition by trip_id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.trip_route_plan` 
                    --WHERE date(created_at) > "2021-07-26"
                    WHERE date(created_at) = '2024-09-24'
                    ),
trip_route_plan_cte as (
                        select distinct created_at,
                        updated_at,
                        bq_upload_time,
                        country_code,
                        trip_id,
                        warehouse_location.outlet_location.latitude as outlet_location_latitude,
                        warehouse_location.outlet_location.longitude as outlet_location_longitude,
                        estimated_distance,
                        distance_covered_by_driver,
                        estimated_duration,
                        duration_covered_by_driver,
                        s.dn_id,
                        s.outlet_location.latitude,
                        s.outlet_location.latitude 
                        from trip_route_plan trp, unnest(sorted_stops) s
                        where index = 1
                        ),
trip_route_plan_stops_cte as (
                        select distinct 
                        trip_id,
                        s.dn_id,
                        s.outlet_location.latitude,
                        s.outlet_location.latitude 
                        from trip_route_plan trp, unnest(stops) s
                        where index = 1
                        )
select *
--max(created_at) as max_created_at_datetime, max(updated_at) as max_updated_at_datetime, max(bq_upload_time) as max_bq_upload_time_datetime
--from trip_route_plan
from trip_route_plan_stops_cte