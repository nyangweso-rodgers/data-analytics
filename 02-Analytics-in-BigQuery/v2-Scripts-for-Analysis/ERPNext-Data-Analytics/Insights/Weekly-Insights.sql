---------------------------------- Weekly Insights -----------------------------------------
with

paid_and_delivered_dns as (
                          SELECT  *
                          FROM `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns` 
                          where territory not in ('Test KE Territory', "Test UG Territory")
                          ),

current_week as (
                  select distinct date_trunc(posting_date, week(monday)) as week,
                  company,
                  count(distinct territory) as territories,
                  count(distinct customer) as customers,
                  count(distinct name) as dns,
                  count(distinct name) / count(distinct customer) as avg_delivery_freq,
                  sum(grand_total) as gmv,
                  sum(grand_total) / count(distinct name) as avg_basket_value,
                  sum(grand_total) / count(distinct customer) as avg_customer_gmv,
                  sum(grand_total) / count(distinct territory) as gmv_per_territory
                  from paid_and_delivered_dns
                  WHERE date_trunc(posting_date, week(monday)) = '2023-05-22'
                  group by 1,2
                  order by 1,2
                  ),
previous_week as (
                  select distinct date_trunc(posting_date, week(monday)) as week,
                  company,
                  count(distinct territory) as territories,
                  count(distinct customer) as customers,
                  count(distinct name) as dns,
                  count(distinct name) / count(distinct customer) as avg_delivery_freq,
                  sum(grand_total) as gmv,
                  sum(grand_total) / count(distinct name) as avg_basket_value,
                  sum(grand_total) / count(distinct customer) as avg_customer_gmv,
                  sum(grand_total) / count(distinct territory) as gmv_per_territory
                  from paid_and_delivered_dns
                  WHERE date_trunc(posting_date, week(monday)) = '2023-05-15'
                  group by 1,2
                  order by 1,2
                  )
select * from previous_week