------------------ outlets journey - v2 -------------
with
list_dates as (
          SELECT * 
          FROM  UNNEST(GENERATE_DATE_ARRAY('2022-02-06', current_date())) AS date
          ),
list_months as ( 
                select distinct date_trunc(date,month) as month 
                from list_dates
                ),
-------------------- Scheduled Delivery Notes --------------------
daily_gmv_report as (
                        SELECT distinct delivery_date,
                        --country_code,
                        --territory_id,
                        --route_id,
                        --route_name,
                        outlet_id,
                        id,
                        --market_developer,
                        --outlet_name,
                        --outlet_phone_number
                        FROM `kyosk-prod.karuru_test.outlets_segmentation` # for testing 
                        --FROM `kyosk-prod.karuru_scheduled_queries.karuru_dns_daily_revenue`  # prod
                        where territory_id not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Test Fresh TZ Territory', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test NG Territory')
                        ),
outlets_first_transactions_cte as (
                                    select distinct outlet_id,
                                    min(date_trunc(delivery_date, month)) as outlet_first_delivery_month
                                    from daily_gmv_report
                                    group by 1
                                    ),
monthly_gmv_cte as (
                    select distinct date_trunc(delivery_date, month) as delivery_month,
                    outlet_id,
                    count(distinct id) as count_monthly_dns
                    from daily_gmv_report
                    group by 1,2
                    ),
------------------------- Delivery Notes --------------------------------
/*
monthly_outlet_last_activity as (
                          select distinct delivery_month,
                          outlet_id,
                          LAST_VALUE(delivery_month) OVER (PARTITION BY outlet_id ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_last_delivery_month,
                          LAST_VALUE(agent_name) OVER (PARTITION BY outlet_id, delivery_month ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_agent_name,
                          LAST_VALUE(territory_id) OVER (PARTITION BY outlet_id, delivery_month ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_territory_id,
                          LAST_VALUE(route_id) OVER (PARTITION BY outlet_id, delivery_month ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_route_id,
                          LAST_VALUE(route_name) OVER (PARTITION BY outlet_id, delivery_month ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_route_name,
                          from daily_delivery_notes_report
                          ),
*/
------------------------------------ outlets ------------------------------
outlets as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.outlets` 
            WHERE date(created_at) >= '2022-02-01'
            ),
outlets_cte as (
                  select distinct date_trunc(date(created_at), month) as created_at_month,
                  --created_by,
                  --updated_by,
                  --market.company as company_id, 
                  --market.market_name as market_name,
                  --market.territory as territory, # all null
                  --route_id,
                  id,
                  --market_developer.first_name as market_developer_first_name,
                  --market_developer.last_name as market_developer_last_name
                  --name,
                  --erp_id,
                  --app_created_on,
                  from outlets
                  where index =1
                  and (market.market_name is not null) 
                  and market.market_name not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Test Fresh TZ Territory', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test NG Territory')
                  ),
-------------------------- Mashup -----------------------------------
all_outlets_cte as (
                    select distinct coalesce(o.id, oft.outlet_id) as outlet_id,
                    case
                      when (o.created_at_month is null) and (oft.outlet_first_delivery_month is not null) then oft.outlet_first_delivery_month
                      when (o.created_at_month is not null) and (o.created_at_month > oft.outlet_first_delivery_month) then oft.outlet_first_delivery_month
                    else o.created_at_month end as outlet_creation_month,
                    oft.outlet_first_delivery_month
                    from outlets_cte o
                    full outer join outlets_first_transactions_cte oft on o.id = oft.outlet_id
                    ),
all_outlets_with_months_cte as (
                                select distinct ao.outlet_id,
                                lm.month,
                                ao.outlet_creation_month,
                                ao.outlet_first_delivery_month
                                from all_outlets_cte ao, list_months lm
                                where  month >= outlet_creation_month
                                ),
all_outlets_with_monthly_transactions as (
                                    select distinct aowm.outlet_id,
                                    aowm.month,
                                    aowm.outlet_creation_month,
                                    aowm.outlet_first_delivery_month,
                                    mgmv.delivery_month,
                                    lag(mgmv.delivery_month)over(partition by aowm.outlet_id order by aowm.month) as previous_delivery_month,
                                    LAST_VALUE(delivery_month IGNORE NULLS)OVER(partition by aowm.outlet_id ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_delivery_month,
                                    coalesce(mgmv.count_monthly_dns, 0) as count_monthly_dns,
                                    sum(mgmv.count_monthly_dns)over(partition by aowm.outlet_id order by aowm.month) as total_dns_count
                                    from all_outlets_with_months_cte aowm
                                    left join monthly_gmv_cte mgmv on aowm.month = mgmv.delivery_month and aowm.outlet_id = mgmv.outlet_id
                                    ),
get_outlets_dimension_groups as (
                                  select aomt.*,
                                  date_diff(month, latest_delivery_month, month) as months_since_last_delivery,
                                  case
                                    when (total_dns_count >= 7) then 'Onboarded'
                                  else 'Not Yet Onboarded' end as onboarding_dimension_group, 
                                  case
                                    when (outlet_first_delivery_month is null) then 'Registered With No Orders'
                                    when (outlet_first_delivery_month is not null) and (delivery_month is null) then 'In-Active'
                                    when (delivery_month is not null) then 'Active'
                                  else 'UNSET' end as active_status_dimension_group,
                                  from all_outlets_with_monthly_transactions aomt
                                  ),
get_outlet_dimensions as (
  select aos.*,
  case
    when (active_status_dimension_group = 'Active') and (delivery_month is not null) and (total_dns_count < 7) then 'New Active'
    when (active_status_dimension_group = 'Active') and (months_since_last_delivery = 0) and (onboarding_dimension_group = 'Onboarded') and (previous_delivery_month is not null) then 'Continued Active'
    when (active_status_dimension_group = 'Active') and (months_since_last_delivery = 0) and (onboarding_dimension_group = 'Onboarded') and (previous_delivery_month is null) then 'Re-Activated'
    when (active_status_dimension_group = 'In-Active') and (total_dns_count < 7) then 'Failed Onboarding'
    when (active_status_dimension_group = 'In-Active') and (months_since_last_delivery >= 3 ) and (onboarding_dimension_group = 'Onboarded') then 'Churned'
    when (active_status_dimension_group = 'In-Active') and (months_since_last_delivery between 0 and 2 ) and (onboarding_dimension_group = 'Onboarded') or (delivery_month is null ) and (onboarding_dimension_group = 'Onboarded') then 'Dormant' 
  else 'UNSET' end as onboarding_dimension
  from get_outlets_dimension_groups aos
  ),
monthly_segmentation_agg_cte as (
                                select distinct month, 
                                onboarding_dimension_group, 
                                onboarding_dimension, 
                                count(distinct outlet_id) as outlet_id
                                from get_outlet_dimensions
                                where month = '2024-08-01'
                                group by 1,2,3
                                order by 1,2,3
                                ),
get_outlets_dimension_group_and_dimension_array_agg_cte as (
                              select month,
                              outlet_id,
                              ARRAY_AGG(onboarding_dimension_group) as dimension_group_with_dimension,
                              ARRAY_AGG(onboarding_dimension) as onboarding_dimension
                              --JSON_OBJECT(ARRAY_AGG(onboarding_dimension_group), ARRAY_AGG(onboarding_dimension)) AS json_data
                              from get_outlet_dimensions
                              group by 1,2
                              )

select *
from get_outlets_dimension_group_and_dimension_array_agg_cte
--where outlet_id = '0CWRTG5N1CTJJ'
--where outlet_id = '0CWFVFAFNWVFJ'
where outlet_id in ('0CWRTG5N1CTJJ', '0CWFVFAFNWVFJ', '0CW7M5CNN7HZ3', '0CW7KGC6YJQQJ', '0CW7D5980441G')
--where month = '2024-08-01'
order by outlet_id, month