------------------ Outlets Dimension and Dimension Groups -------------
with
------------------------------- Date Arrays --------------------------------------
list_dates as (
          SELECT * 
          FROM  UNNEST(GENERATE_DATE_ARRAY('2022-02-06', current_date())) AS date
          ),
list_months as ( 
                select distinct date_trunc(date,month) as month 
                from list_dates
                ),
-------------------- Scheduled Delivery Notes Data --------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                where date(created_at) > "2022-02-06"
                and status in ('PAID','DELIVERED','CASH_COLLECTED')
                ),
delivery_notes_cte as (
                      select distinct 
                      coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                      country_code,
                      territory_id,
                      route_id,
                      route_name,
                      outlet_id,
                      id,
                      agent_name as market_developer,
                      from delivery_notes dn
                      where index = 1
                      ),
daily_gmv_cte as (
                  SELECT distinct delivery_date,
                  format_date('%A', date(delivery_date)) as delivery_day_of_week,
                  country_code,
                  territory_id,
                  route_id,
                  route_name,
                  outlet_id,
                  id,
                  case when market_developer = '' then null else market_developer end as market_developer,
                  --outlet_name,
                  --outlet_phone_number
                  --FROM `kyosk-prod.karuru_test.dimension_test_outlets`  # for testing 
                  --FROM `kyosk-prod.karuru_scheduled_queries.karuru_dns_daily_revenue`  # prod
                  FROM delivery_notes_cte # prod
                  --where territory_id not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Test Fresh TZ Territory', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test NG Territory')
                  ),
outlets_first_transactions_cte as (
                                    select distinct outlet_id,
                                    min(date_trunc(delivery_date, month)) as outlet_first_delivery_month
                                    from daily_gmv_cte
                                    group by 1
                                    ),
monthly_gmv_cte as (
                    select distinct date_trunc(delivery_date, month) as delivery_month,
                    outlet_id,
                    count(distinct id) as monthly_dns_count,
                    count(distinct delivery_date) as monthly_delivery_date_count
                    from daily_gmv_cte
                    group by 1,2
                    ),
monthly_outlet_last_activity_cte as (
                          select distinct date_trunc(delivery_date, month) as delivery_month,
                          outlet_id,
                          --LAST_VALUE(delivery_month) OVER (PARTITION BY outlet_id ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_last_delivery_month,
                          LAST_VALUE(market_developer) OVER (PARTITION BY outlet_id, delivery_date ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_agent_name,
                          LAST_VALUE(country_code) OVER (PARTITION BY outlet_id, delivery_date ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_country_code,
                          LAST_VALUE(territory_id) OVER (PARTITION BY outlet_id, delivery_date ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_territory_id,
                          LAST_VALUE(route_id) OVER (PARTITION BY outlet_id, delivery_date ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_route_id,
                          LAST_VALUE(route_name) OVER (PARTITION BY outlet_id, delivery_date ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_route_name,
                          from daily_gmv_cte
                          ),
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
                  market.company as company_id, 
                  market.market_name as market_name,
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
                  --and (market.market_name is not null) 
                  --and market.market_name not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Test Fresh TZ Territory', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test NG Territory')
                  ),
-------------------------- Mashup -----------------------------------
all_outlets_cte as (
                    select distinct o.company_id,
                    o.market_name,
                    coalesce(o.id, oft.outlet_id) as outlet_id,
                    case
                      when (o.created_at_month is null) and (oft.outlet_first_delivery_month is not null) then oft.outlet_first_delivery_month
                      when (o.created_at_month is not null) and (o.created_at_month > oft.outlet_first_delivery_month) then oft.outlet_first_delivery_month
                    else o.created_at_month end as outlet_creation_month,
                    oft.outlet_first_delivery_month
                    from outlets_cte o
                    full outer join outlets_first_transactions_cte oft on o.id = oft.outlet_id
                    ),
all_outlets_with_months_cte as (
                                select distinct ao.company_id,
                                ao.market_name,
                                ao.outlet_id,
                                lm.month,
                                ao.outlet_creation_month,
                                ao.outlet_first_delivery_month
                                from all_outlets_cte ao, list_months lm
                                where  month >= outlet_creation_month
                                ),
all_outlets_with_monthly_transactions as (
                                    select distinct --aowm.company_id,
                                    --mola.monthly_country_code,
                                    LAST_VALUE(mola.monthly_country_code IGNORE NULLS)OVER(partition by aowm.outlet_id ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_country_code,
                                    aowm.market_name as registration_territory_id, 
                                    --mola.monthly_territory_id,
                                    LAST_VALUE(mola.monthly_territory_id IGNORE NULLS)OVER(partition by aowm.outlet_id ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_territory_id,
                                    aowm.outlet_id,
                                    aowm.month,
                                    aowm.outlet_creation_month,
                                    aowm.outlet_first_delivery_month,
                                    mgmv.delivery_month,
                                    lag(mgmv.delivery_month)over(partition by aowm.outlet_id order by aowm.month) as previous_delivery_month,
                                    LAST_VALUE(mgmv.delivery_month IGNORE NULLS)OVER(partition by aowm.outlet_id ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_delivery_month,
                                    coalesce(mgmv.monthly_dns_count, 0) as monthly_dns_count,
                                    sum(mgmv.monthly_dns_count)over(partition by aowm.outlet_id order by aowm.month) as total_dns_count,
                                    --coalesce(mgmv.monthly_delivery_date_count, 0) as monthly_delivery_date_count
                                    case
                                      when (mgmv.delivery_month is not null) then 'Active'
                                      when (mgmv.delivery_month is null) and (aowm.outlet_first_delivery_month is not null) then 'Not Active'
                                      when (aowm.outlet_first_delivery_month is null) then 'Registered With No Orders' 
                                    else 'UNSET' end as check_monthly_active_status 
                                    from all_outlets_with_months_cte aowm
                                    left join monthly_gmv_cte mgmv on aowm.month = mgmv.delivery_month and aowm.outlet_id = mgmv.outlet_id
                                    left join monthly_outlet_last_activity_cte mola on mgmv.outlet_id = mola.outlet_id and mgmv.delivery_month = mola.delivery_month 
                                    ),
define_dimension_groups_cte as (
                                select 'Onboarding Status'as dimension_group
                                union all (select 'Active Status' as dimension_group)
                                ),
get_outlets_with_dimension_groups_cte as (
                                  select distinct --aomt.company_id,
                                  coalesce(aomt.latest_country_code, 'UNSET') as country_code,
                                  coalesce(aomt.latest_territory_id, aomt.registration_territory_id, 'UNSET') as territory_id,
                                  aomt.outlet_id,
                                  aomt.month,
                                  aomt.outlet_first_delivery_month,
                                  aomt.delivery_month,
                                  aomt.previous_delivery_month,
                                  aomt.monthly_dns_count,
                                  aomt.total_dns_count,
                                  --aomt.monthly_delivery_date_count,
                                  date_diff(month, latest_delivery_month, month) as months_since_last_delivery,
                                  aomt.check_monthly_active_status,
                                  ddg.dimension_group,
                                  from all_outlets_with_monthly_transactions aomt, define_dimension_groups_cte ddg
                                  ),
get_outlets_dimensions_cte as (
      select distinct --aomt.company_id,
      aomt.country_code,
      aomt.territory_id,
      aomt.outlet_id,
      aomt.month,
      aomt.delivery_month,
      aomt.previous_delivery_month,
      aomt.monthly_dns_count,
      aomt.total_dns_count,
      --aomt.monthly_delivery_date_count,
      aomt.months_since_last_delivery,
      aomt.check_monthly_active_status,
      aomt.dimension_group,
      case
        when (dimension_group = 'Onboarding Status') and (total_dns_count >= 7) then 'Onboarded'
        when (dimension_group = 'Onboarding Status') and (total_dns_count < 7) then 'Not Yet Onboarded'
        when (dimension_group = 'Onboarding Status') and (total_dns_count is null) then 'Not Yet Onboarded'

        when (dimension_group = 'Active Status') and (check_monthly_active_status = 'Active') and (total_dns_count < 7) and (delivery_month is not null) then 'New Active'
        when (dimension_group = 'Active Status') and (check_monthly_active_status = 'Active') and (previous_delivery_month is not null) and (total_dns_count >= 7) then 'Continued Active'
        when (dimension_group = 'Active Status') and (check_monthly_active_status = 'Active') and (previous_delivery_month is null) and (total_dns_count >= 7) then 'Re-Activated'
        -- New Active --
        --when (dimension_group = 'Onboarding Status') and (total_dns_count between 1 and 7) and (delivery_month is not null) then 'New Active'
        --when (dimension_group = 'Active Status') and (total_dns_count between 1 and 7) and (delivery_month is not null) then 'New Active'

        -- Failed Onboarding
        --when (dimension_group = 'Onboarding Status') and (total_dns_count < 7) then 'Failed Onboarding'
        --when (dimension_group = 'Active Status') and (total_dns_count between 1 and 7) and (delivery_month is null) and (outlet_first_delivery_month is not null) then 'Failed Onboarding'
        -- onboaded --
        --when (dimension_group = 'Active Status') and (total_dns_count is null ) then 'Registered With No Orders'
        --when (dimension_group = 'Active Status') and (months_since_last_delivery = 0) and (total_dns_count >= 7) and (previous_delivery_month is not null) then 'Continued Active'
        --when (dimension_group = 'Active Status') and (months_since_last_delivery = 0) and (total_dns_count >= 7) and (previous_delivery_month is null) then 'Re-Activated'
        --when (dimension_group = 'Active Status') and (months_since_last_delivery >= 3 ) and (total_dns_count >= 7) then 'Churned'
        --when (dimension_group = 'Active Status') and (months_since_last_delivery between 0 and 2 ) and (total_dns_count >= 7) and (delivery_month is null ) and (total_dns_count >= 7) then 'Dormant'
      else 'UNSET' end as dimension,
      /*
      case
        when (monthly_delivery_date_count = 0) then 'No Transaction'
        when (monthly_delivery_date_count between 1 and 3) then 'Monthly'
        when (monthly_delivery_date_count between 4 and 7) then 'Bi-Weekly'
        when (monthly_delivery_date_count >= 8) then 'Weekly'
      else 'UNSET' end as order_frequency_dimension
      */
      from get_outlets_with_dimension_groups_cte aomt
      ),
/*      
monthly_outlet_segment_array as (
                                  select current_datetime() as created_at, 
                                  month, 
                                  territory_id,
                                  outlet_id, 
                                  ARRAY_AGG(STRUCT(onboarding_dimension_group, onboarding_dimension, order_frequency_dimension)) as dimension_and_dimension_group_summary  
                                  FROM get_outlet_dimensions
                                  GROUP BY month, territory_id, outlet_id
                                  --order by outlet_id, month
                                  ),
current_month_outlet_segment_array as (
                                  select current_datetime() as created_at, 
                                  month, 
                                  territory_id,
                                  outlet_id, 
                                  ARRAY_AGG(STRUCT(onboarding_dimension_group, onboarding_dimension, order_frequency_dimension)) as dimension_and_dimension_group_summary  
                                  FROM get_outlet_dimensions
                                  where month = date_trunc(current_date,month)
                                  GROUP BY month, territory_id, outlet_id
                                  --order by outlet_id, month
                                  )
*/
-------------------- validation -----------------
monthly_customer_journey_agg_cte as (
                                      select distinct month, 
                                      --market_name,
                                      dimension_group,
                                      dimension,
                                      count(distinct outlet_id) as outlets_count
                                      from get_outlets_dimensions_cte
                                      where month = '2024-11-01'
                                      group by 1,2,3
                                      order by 1,2,3
                                      )
--select * from get_outlets_dimension_group_and_dimension_array_agg_cte
--select * from all_outlets_with_monthly_transactions
--select * from get_outlets_dimension_groups
--select * from monthly_outlet_segment_array
--select * from current_month_outlet_segment_array
--where territory_id not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Test Fresh TZ Territory', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test NG Territory')
--where outlet_id = '0CWRTG5N1CTJJ'
--and outlet_id in ('0CWRTG5N1CTJJ', '0CWFVFAFNWVFJ', '0CW7M5CNN7HZ3', '0CW7KGC6YJQQJ', '0CW7D5980441G')
--order by outlet_id, month
--select * from define_dimension_groups_cte
--select * from get_outlets_with_dimension_groups_cte where outlet_id = '0CWRTG5N1CTJJ' order by outlet_id, month

--select * from all_outlets_with_monthly_transactions where outlet_id = '002P9PFSMA79R' order by outlet_id, month

--select * from get_outlets_dimensions_cte where outlet_id = '0CWRTG5N1CTJJ' and month = '2024-11-01' order by outlet_id, month
--select * from get_outlets_dimensions_cte where outlet_id = '0CWFVFAFNWVFJ' and month = '2024-11-01' order by outlet_id, month
--select * from get_outlets_dimensions_cte where outlet_id = '0CWFVFAFNWVFJ' and month = '2024-11-01' order by outlet_id, month
--select * from get_outlets_dimensions_cte where outlet_id = '002TM6QSR9YD2' order by outlet_id, month # test Registered with no order
--select * from get_outlets_dimensions_cte where outlet_id = '002P9PFSMA79R' order by outlet_id, month # test: Failed Onboarding
select * from monthly_customer_journey_agg_cte