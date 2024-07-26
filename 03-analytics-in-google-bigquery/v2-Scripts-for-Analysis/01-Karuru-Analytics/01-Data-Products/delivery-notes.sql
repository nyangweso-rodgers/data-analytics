----------------------- Delivery Notes ------------------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 month)
                where date(created_at) >= date_sub(date_trunc(current_date,month), interval 5 month)
                --where date(created_at) between '2024-01-01' and '2024-07-21'
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
                      select distinct --date(created_at) as 
                      created_at,
                      --updated_at,
                      --bq_upload_time,
                      coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                      country_code,
                      territory_id,
                      route_id,
                      route_name,
                      outlet_id,
                      id,
                      --code,
                      --dn.sale_order_id,
                      --dn.status,
                      --delivery_trip_id,
                      --payment_request_id,
                      agent_name as market_developer,
                      outlet.phone_number as outlet_phone_number,
                      outlet.name as outlet_name,
                      --outlet.outlet_code as outlet_code,
                      --outlet.latitude,
                      --outlet.longitude,
                      --outlet_coordinates[SAFE_OFFSET(0)] as outlet_coordinates_latiude,
                      --outlet_coordinates[SAFE_OFFSET(1)] as outlet_coordinates_longitude,
                      
                      --delivery_coordinates[SAFE_OFFSET(0)] as delivery_coordinates_latitude,
                      --delivery_coordinates[SAFE_OFFSET(1)] as delivery_coordinates_longitude,
                      from delivery_notes dn
                      where index = 1
                      
                      ),
get_latest_outlets_report as (
                        select distinct  outlet_id,
                        LAST_VALUE(outlet_name) OVER (PARTITION BY outlet_id ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_outlet_name,
                        LAST_VALUE(outlet_phone_number) OVER (PARTITION BY outlet_id ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_outlet_phone_number,
                        LAST_VALUE(market_developer) OVER (PARTITION BY outlet_id ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_market_developer,
                        LAST_VALUE(route_name) OVER (PARTITION BY outlet_id ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_route_name,
                        LAST_VALUE(created_at) OVER (PARTITION BY outlet_id ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_dn_creation_datetime,
                        from delivery_notes_cte
                        ),
get_weely_outlets_served as (
                            select distinct date_trunc(delivery_date,week) as delivery_week,
                            outlet_id,
                            country_code,
                            row_number()over(partition by outlet_id order by date_trunc(delivery_date,week) asc) as delivery_week_index,
                            LAST_VALUE(territory_id)over(partition by outlet_id order by date_trunc(delivery_date,week) asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as territory_id,
                            from delivery_notes_cte
                            ),
get_weekly_acquired_and_retained_outlets as (
                                            select *,
                                            case when delivery_week_index = 1 then 'ACQUIRED' else 'RETAINED' end as outlet_status
                                            from get_weely_outlets_served
                                            ),
weekly_acquired_and_retained_outlets_agg as (
                                            select distinct delivery_week,
                                            country_code,
                                            territory_id,
                                            count(distinct(case when outlet_status = 'ACQUIRED' then outlet_id else null end)) as acquired_outlets,
                                            count(distinct(case when outlet_status = 'RETAINED' then outlet_id else null end)) as retained_outlets
                                            from get_weekly_acquired_and_retained_outlets
                                            group by 1,2,3
                                            ),
get_weekly_outlets_agg as (
                            select *,
                            sum(acquired_outlets)over(partition by territory_id order by delivery_week asc) total_outlets
                            from weekly_acquired_and_retained_outlets_agg
                            ),
get_weekly_active_outlets_agg as (
                                    select *,
                                    coalesce(lag(total_outlets)over(partition by territory_id order by delivery_week asc),0) as active_outlets_base
                                    from get_weekly_outlets_agg
                                    )

--weely_outlets_served_with_index as 
select *
--distinct route_id, route_name, min(date(created_at)) as min_created_date, max(date(created_at)) as max_creaed_date
--distinct route_id, count(distinct route_name) as route_name, string_agg(distinct route_name, "/" order by route_name) as route_names
--distinct date_trunc(date(created_at), month) as month
--max(created_at) as max_created_at, max(updated_at) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
--max(date(created_at)) as max_created_at_date, max(date(updated_at)) as max_updated_at_date, max(date(bq_upload_time)) as max_bq_upload_date
--distinct outlet_id, count(distinct route_id) as route_id
--from delivery_notes_cte
from get_latest_outlets_report
--from get_weely_outlets_served
--from get_weekly_active_outlets_agg
--where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
--and country_code = 'KE'
--having route_name > 
--and route_name is null
--and route_id is null