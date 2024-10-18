--------------- scheduled queries ----------------
with
customer_journey_cte as (
                      select *
                      FROM `kyosk-prod.karuru_scheduled_queries.customer_journey`
                      --where  month >= date_sub(date_trunc(current_date, month), interval 1 month) 
                      where  month = '2024-09-01' 
                      ),
monthly_customer_journey_agg_cte as (
                                      select distinct month, 
                                      market_name,
                                      count(distinct id) as outlets_count
                                      from customer_journey_cte
                                      group by 1,2
                                      order by 1,2
                                      )
select * from customer_journey_cte