--------------- scheduled queries ----------------
with
customer_journey_v1 as (
                      select *
                      FROM `kyosk-prod.karuru_scheduled_queries.customer_journey`
                      --where  month >= date_sub(date_trunc(current_date, month), interval 1 month) 
                      where  month = '2024-11-01' 
                      --where id = '002TM6QSR9YD2' # test: 	Registered no Orders
                      --where /*active_status = 'Failed Onboarding' and*/ id = '002P9PFSMA79R'
                      ),
customer_journey_v1_cte as (
                              select distinct market_name,
                              route_id,
                              id,
                              outlet_name,
                              month,
                              months_since_last_delivery,
                              registration_month,
                              first_active_month,
                              first_delivery_month,
                              last_active_month,
                              last_delivery_date,
                              delivery_month,
                              count_of_dns,
                              active_inactive_status,
                              case
                                when active_status = 'Registered no Orders' then 'Registered With No Orders'
                                when active_status = 'Re-activated' then 'Re-Activated'
                              else active_status end as active_status,
                              onboarding_status
                              from customer_journey_v1
                              order by id, month
                              ), 
monthly_customer_journey_agg_cte as (
                                      select distinct month, 
                                      
                                      --market_name,
                                      onboarding_status,
                                      active_status,
                                      count(distinct id) as outlets_count
                                      from customer_journey_v1_cte
                                      group by 1,2,3
                                      order by month, onboarding_status, outlets_count desc, active_status
                                      ),
----------------------- v2 - customer jounery - test ------------
customer_journeey_v2_test_cte as (
                        SELECT distinct month,
                        first_delivery_month,
                        months_since_last_delivery,
                        previous_delivery_month,
                        outlet_id,
                        check_monthly_active_status,
                        dimension_group,
                        dimension
                        FROM `kyosk-prod.karuru_test.dimension_test_output_v5` 
                        --WHERE month = "2024-11-01"
                        WHERE month = date_trunc(current_date, month)
                        --where dimension_group = 'Active Status'
                        --and dimension_group = 'Onboarding Status'
                        ),
--------------------- v1 & v2 data validation -------------------
check_v1_and_v2_cte as (
                          select distinct v1.month,
                          --v1.first_active_month,
                          v1.first_delivery_month as v1_first_delivery_month,
                          v2.first_delivery_month as v2_first_delivery_month,

                          v1.months_since_last_delivery as v1_months_since_last_delivery,
                          v2.months_since_last_delivery as v2_months_since_last_delivery,

                          
                          v1.last_active_month as v1_last_active_month,
                          v2.previous_delivery_month as v2_previous_delivery_month,

                          v1.id as outlet_id,
                          v1.active_status,
                          v2.dimension,
                          v1.active_status = v2.dimension as check_active_status
                          from customer_journey_v1_cte v1
                          left join customer_journeey_v2_test_cte v2 on v1.id = v2.outlet_id and v1.month = v2.month
                          )
--select * from customer_journey_cte
--select * from monthly_customer_journey_agg_cte
--/*
select *
from customer_journeey_v2_test_cte
--where outlet_id = '0CWAR43JXPFYH'
order by outlet_id, month
--*/
/*
select 
distinct active_status, dimension
from check_v1_and_v2_cte 
where check_active_status = false
order by 1,2
*/
