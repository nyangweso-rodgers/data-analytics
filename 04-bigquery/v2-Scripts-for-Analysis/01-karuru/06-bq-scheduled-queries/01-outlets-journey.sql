--------------- scheduled queries ----------------
with
customer_journey as (
                      select *
                      FROM `kyosk-prod.karuru_scheduled_queries.customer_journey`
                      --where  month >= date_sub(date_trunc(current_date, month), interval 1 month) 
                      where  month = '2024-11-01' 
                      --and id = '0CWRTG5N1CTJJ'
                      --and id = '0CWFVFAFNWVFJ'
                      --where id = '002TM6QSR9YD2' # test: 	Registered no Orders
                      --where /*active_status = 'Failed Onboarding' and*/ id = '002P9PFSMA79R'
                      ),
customer_journey_cte as (
                              select distinct market_name,
                              route_id,
                              id,
                              outlet_name,
                              month,
                              registration_month,
                              first_active_month,
                              first_delivery_month,
                              last_active_month,
                              last_delivery_date,
                              delivery_month,
                              months_since_last_delivery,
                              count_of_dns,
                              active_inactive_status,
                              active_status,
                              onboarding_status
                              from customer_journey
                              order by id, month
                              ), 
monthly_customer_journey_agg_cte as (
                                      select distinct month, 
                                      --market_name,
                                      onboarding_status,
                                      active_status,
                                      count(distinct id) as outlets_count
                                      from customer_journey_cte
                                      group by 1,2,3
                                      order by 1,2,3
                                      )
--select * from customer_journey_cte
select * from monthly_customer_journey_agg_cte