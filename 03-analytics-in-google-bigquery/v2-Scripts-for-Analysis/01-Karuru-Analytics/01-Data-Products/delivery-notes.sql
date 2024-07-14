----------------------- Delivery Notes ------------------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 day)
                where date(created_at) > date_sub(current_date, interval 2 month)
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
                          select distinct --date(created_at) as 
                          created_at,
                          updated_at,
                          bq_upload_time,
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
                          --agent_name as market_developer,
                          --outlet.phone_number,
                          
                          --outlet.name as outlet_name,
                          --outlet.outlet_code as outlet_code,
                          outlet.latitude,
                          outlet.longitude,
                          LAST_VALUE(agent_name) OVER (PARTITION BY route_name ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as market_developer_name
                          from delivery_notes dn
                          where index = 1
                          --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                          --and dni.status = 'ITEM_FULFILLED'
                          --and outlet_id = '0CW6NN3588WDD'
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
--distinct date_trunc(date(created_at), month) as month
--max(created_at) as max_created_at, max(updated_at) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
--distinct outlet_id, count(distinct route_id) as route_id
--from delivery_notes_cte
--from get_weely_outlets_served
from get_weekly_active_outlets_agg
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
and country_code = 'KE'
order by territory_id, delivery_week