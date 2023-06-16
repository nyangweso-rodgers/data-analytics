----------------------------------BRD - Commercial Global v5-------------------------------------------------
with 
-------------------------------- Date Variables ----------------------------------------------------
dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date),
daily_sale_days as (select * from dates ),

dates_2 as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-02-06',last_day(CURRENT_DATE()), INTERVAL 1 DAY)) AS date),
monthly_sale_days as ( select distinct date_trunc(date,month) as month, count(distinct date) as days_in_month from dates_2 group by 1 order by 1),

vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  --SELECT DATE '2023-05-08' as current_start_date, DATE '2023-05-14' as current_end_date ),
date_vars as (
                select *,

                date_trunc(current_end_date, week(MONDAY)) as current_start_week,
                date_add(date_trunc(current_end_date, week(MONDAY)), interval 6 day) as current_end_week,

                date_sub(date_trunc(current_end_date, week(MONDAY)), interval 1 week) as previous_start_week, 
                date_sub(date_trunc(current_end_date, week(MONDAY)), interval 1 day) as previous_end_week ,

                date_sub(case when (current_end_date = current_date()) or (current_date() between current_start_date and current_end_date) then current_date() else date_add(date_trunc(current_end_date, week(MONDAY)), interval 6 day) end, interval 30 day) as current_start_month,
                case when (current_end_date = current_date()) or (current_date() between current_start_date and current_end_date) then current_date() else date_add(date_trunc(current_end_date, week(MONDAY)), interval 6 day) end as current_end_month,

                date_sub(date_sub(date_trunc(current_end_date, week(MONDAY)), interval 1 day), interval 30 day) as previous_start_month,
                date_sub(date_trunc(current_end_date, week(MONDAY)), interval 1 day) as previous_end_month,

                date_sub(date_trunc(current_end_date, week(MONDAY)), interval 2 week) as previous_previous_start_week, 
                date_add(date_sub(date_trunc(current_end_date, week(MONDAY)), interval 2 week), interval 6 day) as previous_previous_end_week ,

                case when date_trunc(current_date(),month) = date_trunc(current_end_date, month) then date_trunc(current_end_date, month) else date_trunc(current_start_date, month) end  as month_to_date_start,
                case when (current_end_date = current_date()) or (current_date() between current_start_date and current_end_date) then current_date() 
                else current_end_date end as month_to_date_end,

                date_sub(case when date_trunc(current_date(),month) = date_trunc(current_end_date, month) then date_trunc(current_end_date, month) else date_trunc(current_start_date, month) end , interval 1 month) as previous_month_to_date_start,
                date_sub(case when date_trunc(current_date(),month) = date_trunc(current_end_date, month) then date_trunc(current_end_date, month) else date_trunc(current_start_date, month) end, interval 1 day) as previous_month_to_date_end,

                case when date_trunc(current_date(),week(MONDAY)) = date_trunc(current_end_date, week(MONDAY)) then date_diff(current_date(),date_trunc(current_end_date, week(MONDAY)) , day) else date_diff(date_add(date_trunc(current_end_date, week(MONDAY)), interval 7 day),date_trunc(current_end_date, week(MONDAY)),day) end as current_wtd_days,

                case when date_trunc(current_date(),month) = date_trunc(current_end_date, month) and date_trunc(current_date(),week(MONDAY)) = date_trunc(current_end_date, week(MONDAY)) then date_diff(current_date(),date_trunc(current_end_date, month), day) else date_diff(current_end_date,date_trunc(current_end_date, month) - 1,day) end as mtd_days
              
                from vars
                ),
fx_rate as (select * from `uploaded_tables.uploaded_table_fx_rate_conversion_v5`),
commercial_kpis as (select * from `uploaded_tables.uploaded_table_commercial_kpis_v5` where deliverable = 'Commercial'),
targets as (select * from `uploaded_tables.upload_business_kpi_targets_v3` ),
cancelled_dns as (select * from `erp_scheduled_queries.erp_dns_cancellations`),
targets_data as (
                  select distinct t.start_date,
                  t.end_date,
                  t.registered_retailers,
                  t.daily_activated_retailers,
                  t.active_outlets_weekly,
                  t.activity_rate_weekly,
                  --t.basket_size,
                  t.cancellations,
                  t.kyosk_app_adoption_order_count,
                  t.kyosk_app_adoption_order_value,
                  t.cash_flow_days_inventory_outstanding,
                  t.market_development_cost,
                  fr.fx_rate_dfn, 
                  fr.fx_rate,
                  case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.revenue_growth else null end as revenue_growth,
                  case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.margin else null end as margin,
                  case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.daily_active_outlets_monthly else null end as daily_active_outlets_monthly,
                  case when t.country not in ('NIGERIA') then t.activity_rate_monthly else null end as activity_rate_monthly,
                  case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.order_frequency_monthly else null end as order_frequency_monthly,
                  case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.daily_order_count else null end as order_count,
                  (case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.basket_size else null end) / fr.fx_rate as basket_value,
                  (case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.daily_revenue_per_territory else 0 end) / fr.fx_rate as daily_revenue_per_territory_in_usd,
                  (case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.revenue_per_territory else 0 end) / fr.fx_rate as revenue_per_territory_in_usd,
                  (case when t.country in ('KENYA','UGANDA','TANZANIA','NIGERIA') then t.daily_revenue else 0 end) / fr.fx_rate as daily_revenue_in_usd
                  from targets t
                  left join fx_rate fr on t.company = fr.company and case when fr.fx_rate_dfn = 'Actual Rate' then t.start_date = fr.start_date and t.end_date = fr.end_date else t.start_date between fr.start_date and fr.end_date and t.end_date between fr.start_date and fr.end_date end
                  --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
                  ),
targets_with_daily_sales_dates as (select * from targets_data ,daily_sale_days),
sales_orders as (select distinct so.transaction_date,
                so.sales_order,
                so.customer,
                so.created_on_app,
                fr.fx_rate_dfn,
                fr.fx_rate,
                sum(so.grand_total) as grand_total,
                sum(so.grand_total)/ fr.fx_rate as grand_total_in_usd
                from `erp_scheduled_queries.erp_sales_orders` so,fx_rate fr
                where workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
                and so.transaction_date between fr.start_date and fr.end_date and so.company = fr.company
                group by 1,2,3,4,5,6
                ),
gross_margin as (select distinct gm.creation_date,
                fr.fx_rate_dfn,
                fr.fx_rate,
                sum(gm.base_net_amount_of_sales_invoice) / fr.fx_rate as base_net_amount_in_usd,
                sum(gm.total_incoming_rate) / fr.fx_rate as total_incoming_rate_in_usd
                from `erp_scheduled_queries.erp_front_margin_v2` gm,fx_rate fr
                where gm.creation_date between fr.start_date and fr.end_date and gm.company = fr.company
                group by 1,2,3
                ),
customers as (select distinct c.name,
              date(c.creation) as date_created,
              fr.fx_rate_dfn,
              from `erp_scheduled_queries.erp_customers` c,fx_rate fr
              where date(creation) between fr.start_date and fr.end_date and c.company =fr.company
              ),
paid_and_delivered_dns as (select distinct pdns.posting_date,
                          pdns.name as delivery_note,
                          pdns.customer, 
                          pdns.territory,
                          fr.fx_rate_dfn,
                          fr.fx_rate, 
                          sum(pdns.grand_total) as grand_total,
                          sum(pdns.grand_total)/ fr.fx_rate as grand_total_in_usd
                          from `erp_scheduled_queries.erp_paid_and_delivered_dns` pdns ,fx_rate fr
                          where pdns.posting_date between fr.start_date and fr.end_date and pdns.company = fr.company
                          group by 1,2,3,4,5,6
                          ),
activated_retailers_dfn as (
                            select distinct customer,
                            fx_rate_dfn,
                            posting_date,
                            count(distinct posting_date) as count_of_posting_dates
                            from paid_and_delivered_dns
                            group by 1,2,3
                            having (count_of_posting_dates >= 1)
                            ),
-------------------Previous Month to Date Sales-----------------------------------------------
previous_month_to_date_sales as (
                                    select distinct fx_rate_dfn,
                                    sum(grand_total_in_usd) as revenue_in_usd
                                    from paid_and_delivered_dns,date_vars
                                    where posting_date between previous_month_to_date_start and previous_month_to_date_end
                                    group by 1
                                  ),
---------------------------------Month to Date ------------------------------------------------------
month_to_date_targets as (
                          select distinct fx_rate_dfn,
                          sum(daily_revenue_in_usd) as daily_revenue_in_usd,
                          --avg(daily_revenue_per_territory_in_usd) as daily_revenue_per_territory_in_usd,
                          avg(revenue_growth) as revenue_growth,
                          avg(margin) as margin,
                          sum(registered_retailers) as registered_retailers,
                          --sum(daily_active_outlets_monthly) as daily_active_outlets_monthly,
                          avg(activity_rate_monthly) as activity_rate_monthly,
                          avg(order_frequency_monthly) as order_frequency_monthly,
                          avg(basket_value) as basket_value, 
                          sum(order_count) as order_count,
                          avg(cancellations) as cancellations,
                          avg(kyosk_app_adoption_order_count) as kyosk_app_adoption_order_count,
                          avg(kyosk_app_adoption_order_value) as kyosk_app_adoption_order_value,
                          avg(cash_flow_days_inventory_outstanding) as cash_flow_days_inventory_outstanding,
                          sum(market_development_cost) as market_development_cost
                          from targets_with_daily_sales_dates t, date_vars 
                          where (date between t.start_date and t.end_date) and (date between month_to_date_start and (case when current_date() = month_to_date_end then date_sub(month_to_date_end,interval 1 day) else month_to_date_end end))
                          group by 1
                        ),
month_to_date_targets_for_projected_rev as (select distinct fx_rate_dfn,
                                            sum(daily_revenue_in_usd) as daily_revenue_in_usd,
                                            sum(daily_activated_retailers) as daily_activated_retailers,
                                            sum(daily_active_outlets_monthly) as daily_active_outlets_monthly
                                            from targets_with_daily_sales_dates t, date_vars 
                                            where (date between t.start_date and t.end_date) and month_to_date_start = t.start_date
                                            group by 1
                                            ),
month_to_date_sales as (
                        select distinct fx_rate_dfn,
                        count(distinct delivery_note) as count_of_dns,
                        count(distinct customer) as count_of_customers, 
                        count(distinct territory) as count_of_territories,
                        sum(grand_total_in_usd) as revenue_in_usd,
                        (sum(grand_total_in_usd) / avg(mtd_days) ) * avg(msd.days_in_month) as projected_revenue_in_usd
                        from paid_and_delivered_dns,date_vars,monthly_sale_days msd
                        where posting_date between month_to_date_start and month_to_date_end
                        and date_trunc(posting_date, month) = msd.month
                        group by 1
                        ),
month_to_date_gross_margin as (
                               select distinct fx_rate_dfn,
                                sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                                sum(base_net_amount_in_usd - total_incoming_rate_in_usd) as gross_margin_in_usd,
                                from gross_margin gm,date_vars 
                                where gm.creation_date between month_to_date_start and month_to_date_end
                                group by 1 
                              ),
month_to_date_registered_customers as (
                                        select distinct fx_rate_dfn,
                                        count(distinct name) as mtd_count_of_registered_customers 
                                        from customers, date_vars
                                        where date_created <= month_to_date_end
                                        group by 1
                                      ),
month_to_date_activated_retailers as (
                                      select distinct fx_rate_dfn,
                                      count(distinct customer) as count_of_activated_retailers
                                      from activated_retailers_dfn,date_vars
                                      where posting_date <=month_to_date_end
                                      group by 1
                                      ),
month_to_date_orders as (
                        select distinct fx_rate_dfn,
                        count(distinct customer) as count_of_customers,
                        count(distinct sales_order) as count_of_orders,
                        count(distinct (case when created_on_app = 'Duka App' then sales_order else null end)) as count_of_kyosk_app_orders,
                        sum(case when created_on_app = 'Duka App' then grand_total_in_usd else null end) as kyosk_app_revenue_in_usd,
                        sum(grand_total_in_usd) as ordered_amount_in_usd
                        from sales_orders ,date_vars
                        where transaction_date between month_to_date_start and month_to_date_end
                        group by 1
                        ),
month_to_date_cancelled_dns as (
                          select distinct case when ex_rate_dfn = 'Actual Rate' then 'Actual Rate'
                                when ex_rate_dfn = 'Budget Rate (FY2023)' then 'Budget Rate FY 2023 (Apr. 2022 - March 2023)'
                                when ex_rate_dfn = 'Budget Rate (FY2024)' then 'Budget Rate FY 2024 (Apr. 2023 - March 2024)'
                                else null end as fx_rate_dfn,
                          sum(cancelled_amount_in_usd) as cancelled_amount_in_usd,
                          sum(amount_in_usd) as amount_in_usd
                          from cancelled_dns ,date_vars
                          where posting_date between month_to_date_start and month_to_date_end
                          group by 1
                          ),
--------------------------------Previous previous Week Sales-----------------------------------------
previous_previous_week_sales as (
                              select distinct fx_rate_dfn,
                              sum(grand_total_in_usd) as revenue_in_usd
                              from paid_and_delivered_dns,date_vars
                              where posting_date between previous_previous_start_week and previous_previous_end_week
                              group by 1
                              ),
-------------------------------Previous Week-----------------------------------------------------
previous_week_targets as (
                      select distinct fx_rate_dfn,
                      sum(daily_revenue_in_usd) as daily_revenue_in_usd,
                      --sum(daily_revenue_per_territory_in_usd) as daily_revenue_per_territory_in_usd,
                      avg(revenue_growth) as revenue_growth,
                      avg(margin) as margin,
                      sum(registered_retailers) as registered_retailers,
                      sum(daily_active_outlets_monthly) as active_outlets_weekly,
                      avg(activity_rate_weekly) as activity_rate_weekly,
                      sum(order_count) as order_count,
                      avg(basket_value) as basket_value,
                      avg(cancellations) as cancellations,
                      avg(kyosk_app_adoption_order_count) as kyosk_app_adoption_order_count,
                      avg(kyosk_app_adoption_order_value) as kyosk_app_adoption_order_value,
                      avg(cash_flow_days_inventory_outstanding) as cash_flow_days_inventory_outstanding,
                      sum(market_development_cost) as market_development_cost
                      from targets_with_daily_sales_dates t, date_vars
                      where (date between t.start_date and t.end_date) and (date between date_vars.previous_start_week and date_vars.previous_end_week)
                      group by 1
                      ),
previous_month_targets as (
                            select distinct fx_rate_dfn,
                            sum(daily_activated_retailers) as daily_activated_retailers,
                            sum(daily_active_outlets_monthly) as daily_active_outlets_monthly,
                            avg(activity_rate_monthly) as activity_rate_monthly,
                            avg(order_frequency_monthly) as order_frequency_monthly,
                            from targets_with_daily_sales_dates t, date_vars
                            where (date between t.start_date and t.end_date) and (date between date_vars.previous_start_month and date_vars.previous_end_month) 
                            group by 1
                          ),
previous_week_sales as (
                        select distinct fx_rate_dfn,
                        count(distinct delivery_note) as count_of_dns,
                        count(distinct customer) as count_of_customers, 
                        count(distinct territory) as count_of_territories,
                        sum(grand_total_in_usd) as revenue_in_usd,
                        from paid_and_delivered_dns,date_vars
                        where posting_date between previous_start_week and previous_end_week
                        group by 1
                        ),
previous_week_gross_margin as (
                                select distinct fx_rate_dfn,
                                sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                                sum(base_net_amount_in_usd - total_incoming_rate_in_usd) as gross_margin_in_usd,
                                from gross_margin gm,date_vars 
                                where gm.creation_date between previous_start_week and previous_end_week
                                group by 1
                                ),
previous_registered_customers as ( 
                                  select distinct fx_rate_dfn,
                                  count(distinct name) as previous_count_of_registered_customers 
                                  from customers, date_vars
                                  where date_created <= previous_end_week
                                  group by 1
                                  ),
previous_week_activated_retailers as (
                                      select distinct fx_rate_dfn,
                                      count(distinct customer) as count_of_activated_retailers
                                      from activated_retailers_dfn,date_vars
                                      where posting_date <=previous_end_week
                                      group by 1
                                      ),
previous_week_orders as (
                        select distinct fx_rate_dfn,
                        count(distinct customer) as count_of_customers,
                        count(distinct sales_order) as count_of_orders,
                        count(distinct (case when created_on_app = 'Duka App' then sales_order else null end)) as count_of_kyosk_app_orders,
                        sum(case when created_on_app = 'Duka App' then grand_total_in_usd else null end) as kyosk_app_revenue_in_usd,
                        sum(grand_total_in_usd) as ordered_amount_in_usd
                        from sales_orders ,date_vars
                        where transaction_date between previous_start_week and previous_end_week
                        group by 1
                        ),
previous_cancelled_dns as (
                          select distinct case when ex_rate_dfn = 'Actual Rate' then 'Actual Rate'
                                when ex_rate_dfn = 'Budget Rate (FY2023)' then 'Budget Rate FY 2023 (Apr. 2022 - March 2023)'
                                when ex_rate_dfn = 'Budget Rate (FY2024)' then 'Budget Rate FY 2024 (Apr. 2023 - March 2024)'
                                else null end as fx_rate_dfn,
                          sum(cancelled_amount_in_usd) as cancelled_amount_in_usd,
                          sum(amount_in_usd) as amount_in_usd
                          from cancelled_dns ,date_vars
                          where posting_date between previous_start_week and previous_end_week
                          group by 1
                          ),
------------------------------Previous Month---------------------------------------------------------
previous_month_sales as (
                          select distinct fx_rate_dfn,
                          count(distinct delivery_note) as count_of_dns,
                          count(distinct customer) as count_of_customers, 
                          from paid_and_delivered_dns,date_vars
                          where posting_date between previous_start_month and previous_end_month
                          group by 1
                        ),
previous_month_active_customers as (
                                    select distinct fx_rate_dfn,
                                    count(distinct customer) as count_of_customers
                                    from paid_and_delivered_dns,date_vars
                                    where posting_date between previous_start_month and previous_end_month
                                    group by 1
                                    ),
previous_month_registered_customers as ( 
                                        select distinct fx_rate_dfn,
                                        count(distinct name) as previous_count_of_registered_customers
                                        from customers, date_vars
                                        where date_created <= previous_end_month
                                        group by 1
                                      ),
previous_month_orders as (
                          select distinct fx_rate_dfn,
                          count(distinct customer) as count_of_customers,
                          count(distinct sales_order) as count_of_orders,
                          sum(grand_total_in_usd) as ordered_amount_in_usd
                          from sales_orders ,date_vars
                          where transaction_date between previous_start_month and previous_end_month
                          group by 1
                          ),
-------------------------------Current Week-----------------------------------------------------
current_week_targets as (
                      select distinct fx_rate_dfn,
                      sum(daily_revenue_in_usd) as daily_revenue_in_usd,
                      --sum(daily_revenue_per_territory_in_usd) as daily_revenue_per_territory_in_usd,
                      avg(revenue_growth) as revenue_growth,
                      avg(margin) as margin,
                      sum(registered_retailers) as registered_retailers,  
                      avg(activity_rate_monthly) as activity_rate_monthly,
                      avg(daily_active_outlets_monthly) as active_outlets_weekly,
                      avg(activity_rate_weekly) as activity_rate_weekly,
                      avg(order_frequency_monthly) as order_frequency_monthly,
                      sum(order_count) as order_count,
                      avg(basket_value) as basket_value,
                      avg(cancellations) as cancellations,
                      avg(kyosk_app_adoption_order_count) as kyosk_app_adoption_order_count,
                      avg(kyosk_app_adoption_order_value) as kyosk_app_adoption_order_value,
                      avg(cash_flow_days_inventory_outstanding) as cash_flow_days_inventory_outstanding,
                      sum(market_development_cost) as market_development_cost
                      from targets_with_daily_sales_dates t, date_vars
                      where (date between t.start_date and t.end_date) and (date between date_vars.current_start_week and (case when current_date() = current_end_date then date_sub(current_date(), interval 1 day) else date_vars.current_end_week end)) 
                      group by 1
                      ),
current_week_targets_for_projected_rev as (select distinct fx_rate_dfn,
                                            sum(daily_revenue_in_usd) as daily_revenue_in_usd,
                                            from targets_with_daily_sales_dates t, date_vars 
                                            where (date between t.start_date and t.end_date) and (date between date_vars.current_start_week and date_vars.current_end_week)
                                            group by 1
                                            ),
current_month_targets as (
                            select distinct fx_rate_dfn,
                            sum(daily_activated_retailers) as daily_activated_retailers,
                            sum(daily_active_outlets_monthly) as daily_active_outlets_monthly,
                            avg(activity_rate_monthly) as activity_rate_monthly,
                            avg(order_frequency_monthly) as order_frequency_monthly,
                            from targets_with_daily_sales_dates t, date_vars
                            where (date between t.start_date and t.end_date) and (date between date_vars.current_start_month and date_vars.current_end_month) 
                            group by 1
                          ),
current_week_sales as (
                        select distinct fx_rate_dfn,
                        count(distinct delivery_note) as count_of_dns,
                        count(distinct customer) as count_of_customers,
                        count(distinct territory) as count_of_territories,
                        sum(grand_total_in_usd) as revenue_in_usd,
                        (sum(grand_total_in_usd) / avg(current_wtd_days)) * 7 as projected_revenue_in_usd
                        from paid_and_delivered_dns,date_vars
                        where posting_date between current_start_week and current_end_week
                        group by 1
                        ),
current_week_gross_margin as (
                              select distinct fx_rate_dfn,
                              sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                              sum(base_net_amount_in_usd - total_incoming_rate_in_usd) as gross_margin_in_usd,
                              from gross_margin gm,date_vars 
                              where gm.creation_date between current_start_week and current_end_week
                              group by 1
                              ),
current_registered_customers as ( 
                                  select distinct fx_rate_dfn,
                                  count(distinct name) as current_count_of_registered_customers 
                                  from customers, date_vars
                                  where date_created <= current_end_week
                                  group by 1
                                  ),
current_week_activated_retailers as (
                                      select distinct fx_rate_dfn,
                                      count(distinct customer) as count_of_activated_retailers
                                      from activated_retailers_dfn,date_vars
                                      where posting_date <=current_end_week
                                      group by 1
                                      ),
current_week_orders as (
                        select distinct fx_rate_dfn,
                        count(distinct customer) as count_of_customers,
                        count(distinct sales_order) as count_of_orders,
                        count(distinct (case when created_on_app = 'Duka App' then sales_order else null end)) as count_of_kyosk_app_orders,
                        sum(case when created_on_app = 'Duka App' then grand_total_in_usd else null end) as kyosk_app_revenue_in_usd,
                        sum(grand_total_in_usd) as ordered_amount_in_usd
                        from sales_orders ,date_vars
                        where transaction_date between current_start_week and current_end_week
                        group by 1
                        ),
current_cancelled_dns as (
                          select distinct case when ex_rate_dfn = 'Actual Rate' then 'Actual Rate'
                                when ex_rate_dfn = 'Budget Rate (FY2023)' then 'Budget Rate FY 2023 (Apr. 2022 - March 2023)'
                                when ex_rate_dfn = 'Budget Rate (FY2024)' then 'Budget Rate FY 2024 (Apr. 2023 - March 2024)'
                                else null end as fx_rate_dfn,
                          sum(cancelled_amount_in_usd) as cancelled_amount_in_usd,
                          sum(amount_in_usd) as amount_in_usd
                          from cancelled_dns ,date_vars
                          where posting_date between current_start_week and current_end_week
                          group by 1
                        ),
----------------------------Current Month---------------------------------------------
current_month_sales as (
                          select distinct fx_rate_dfn,
                          count(distinct delivery_note) as count_of_dns,
                          count(distinct customer) as count_of_customers, 
                          from paid_and_delivered_dns,date_vars
                          where posting_date between current_start_month and current_end_month
                          group by 1
                        ),
current_month_active_customers as (
                                    select distinct fx_rate_dfn,
                                    count(distinct customer) as count_of_customers
                                    from paid_and_delivered_dns,date_vars
                                    where posting_date between current_start_month and current_end_month
                                    group by 1
                                  ),
current_month_registered_customers as ( 
                                      select distinct fx_rate_dfn,
                                      count(distinct name) as current_count_of_registered_customers
                                      from customers, date_vars
                                      where date_created <= current_end_month
                                      group by 1
                                      ),
current_month_orders as (
                          select distinct fx_rate_dfn,
                          count(distinct customer) as count_of_customers,
                          count(distinct sales_order) as count_of_orders,
                          sum(grand_total_in_usd) as ordered_amount_in_usd
                          from sales_orders ,date_vars
                          where transaction_date between current_start_month and current_end_month
                          group by 1
                        ),
------------------------------Final Model-------------------------------------------------------

final_model as ( select ck.*,
                  '' as blank,

                    case 
                      when kpi_name = "revenue_(usd)" then mtdt.daily_revenue_in_usd
                      when kpi_name = "projected_revenue" then mtdtpr.daily_revenue_in_usd
                      when kpi_name = "revenue_per_territory" then (mtdt.daily_revenue_in_usd /  mtds.count_of_territories)
                      when kpi_name = "revenue_growth_(%)" then mtdt.revenue_growth
                      when kpi_name = "front_margin_(%)" then mtdt.margin
                      when kpi_name = "activated_retailers" then mtdtpr.daily_activated_retailers
                      when kpi_name = "active_outlets_(monthly)" then mtdtpr.daily_active_outlets_monthly
                      when kpi_name = "activity_rate_(monthly)" then mtdt.activity_rate_monthly
                      when kpi_name = "order_frequency_(monthly)" then mtdt.order_frequency_monthly
                      when kpi_name = "basket_value" then mtdt.basket_value
                      when kpi_name = "order_count" then mtdt.order_count
                      when kpi_name = "cancellations" then mtdt.cancellations
                      when kpi_name = "kyosk_app_(order_count)" then mtdt.kyosk_app_adoption_order_count
                      when kpi_name = "kyosk_app_(order_value)" then mtdt.kyosk_app_adoption_order_value
                      when kpi_name = "cash_flow_(days_inventory_outstanding)" then mtdt.cash_flow_days_inventory_outstanding
                    else null end as month_to_date_targets,

                    case 
                      when kpi_name = "revenue_(usd)" then mtds.revenue_in_usd
                      when kpi_name = "projected_revenue" then mtds.projected_revenue_in_usd
                      when kpi_name = "revenue_growth_(%)" then (mtds.projected_revenue_in_usd / pmtds.revenue_in_usd - 1) * 100
                      when kpi_name = "revenue_per_territory" then (mtds.revenue_in_usd / mtds.count_of_territories)
                      when kpi_name = "front_margin_(%)" then (mtdgm.gross_margin_in_usd / mtdgm.base_net_amount_in_usd) * 100
                      --when kpi_name = "registered_retailers" then mtdrc.mtd_count_of_registered_customers
                      when kpi_name = "activated_retailers" then mtdar.count_of_activated_retailers
                      when kpi_name = "active_outlets_(monthly)" then mtds.count_of_customers
                      when kpi_name = "activity_rate_(monthly)" then (mtds.count_of_customers / mtdar.count_of_activated_retailers) * 100
                      --when kpi_name = "active_outlets_(weekly)" then cs.count_of_customers
                      --when kpi_name = "activity_rate_(weekly)" then (cs.count_of_customers / mtdar.count_of_activated_retailers) * 100
                      when kpi_name = "order_frequency_(monthly)" then round(mtds.count_of_dns / mtds.count_of_customers,1)
                      when kpi_name = "basket_value" then (mtds.revenue_in_usd / mtds.count_of_dns)
                      when kpi_name = "order_count" then mtds.count_of_dns
                      when kpi_name = "cancellations" then (mtdcd.cancelled_amount_in_usd / mtdcd.amount_in_usd) * 100
                      when kpi_name = "kyosk_app_(order_count)" then (mtdo.count_of_kyosk_app_orders / mtdo.count_of_orders) * 100
                      when kpi_name = "kyosk_app_(order_value)" then (mtdo.kyosk_app_revenue_in_usd / mtdo.ordered_amount_in_usd) * 100
                    else null end as month_to_date_actual,

                    case
                      when kpi_name = "revenue_(usd)" then pwt.daily_revenue_in_usd
                      when kpi_name = "revenue_per_territory" then (pwt.daily_revenue_in_usd / ps.count_of_territories)
                      --when kpi_name = "revenue_growth_(%)" then pwt.revenue_growth
                      when kpi_name = "front_margin_(%)" then pwt.margin
                      when kpi_name = "activated_retailers" then pmt.daily_activated_retailers
                      when kpi_name = "active_outlets_(monthly)" then pmt.daily_active_outlets_monthly
                      when kpi_name = "activity_rate_(monthly)" then pmt.activity_rate_monthly
                      --when kpi_name = "active_outlets_(weekly)" then pwt.active_outlets_weekly
                      when kpi_name = "order_frequency_(monthly)" then mtdt.order_frequency_monthly
                      when kpi_name = "basket_value" then pwt.basket_value
                      when kpi_name = "order_count" then pwt.order_count
                      when kpi_name = "cancellations" then pwt.cancellations
                      when kpi_name = "kyosk_app_(order_count)" then pwt.kyosk_app_adoption_order_count
                      when kpi_name = "kyosk_app_(order_value)" then pwt.kyosk_app_adoption_order_value
                      when kpi_name = "cash_flow_(days_inventory_outstanding)" then pwt.cash_flow_days_inventory_outstanding
                    else null end as previous_week_targets,

                    case
                      when kpi_name = "revenue_(usd)" then cwt.daily_revenue_in_usd
                      when kpi_name = "projected_revenue" then cwtpr.daily_revenue_in_usd
                      when kpi_name = "revenue_per_territory" then (cwt.daily_revenue_in_usd / cs.count_of_territories)
                      --when kpi_name = "revenue_growth_(%)" then cwt.revenue_growth
                      when kpi_name = "front_margin_(%)" then cwt.margin
                      when kpi_name = "activated_retailers" then cmt.daily_activated_retailers
                      when kpi_name = "active_outlets_(monthly)" then cmt.daily_active_outlets_monthly
                      when kpi_name = "activity_rate_(monthly)" then cmt.activity_rate_monthly
                      --when kpi_name = "active_outlets_(weekly)" then cwt.active_outlets_weekly
                      when kpi_name = "order_frequency_(monthly)" then mtdt.order_frequency_monthly
                      when kpi_name = "basket_value" then cwt.basket_value
                      when kpi_name = "order_count" then cwt.order_count
                      when kpi_name = "cancellations" then cwt.cancellations
                      when kpi_name = "kyosk_app_(order_count)" then cwt.kyosk_app_adoption_order_count
                      when kpi_name = "kyosk_app_(order_value)" then cwt.kyosk_app_adoption_order_value
                      when kpi_name = "cash_flow_(days_inventory_outstanding)" then cwt.cash_flow_days_inventory_outstanding
                    else null end as current_week_targets,
                    
                    case 
                       when kpi_name = "revenue_(usd)" then ps.revenue_in_usd
                       when kpi_name = "revenue_growth_(%)" then (ps.revenue_in_usd / ppws.revenue_in_usd - 1) * 100
                       when kpi_name = "revenue_per_territory" then (ps.revenue_in_usd / ps.count_of_territories)
                       when kpi_name = "front_margin_(%)" then (pwgm.gross_margin_in_usd / pwgm.base_net_amount_in_usd) * 100
                       --when kpi_name = "registered_retailers" then prc.previous_count_of_registered_customers
                       when kpi_name = "activated_retailers" then pwar.count_of_activated_retailers
                       when kpi_name = "active_outlets_(monthly)" then pmac.count_of_customers
                       when kpi_name = "activity_rate_(monthly)" then (pmac.count_of_customers / pwar.count_of_activated_retailers) * 100
                       when kpi_name = "active_outlets_(weekly)" then ps.count_of_customers
                       when kpi_name = "activity_rate_(weekly)" then (ps.count_of_customers / pwar.count_of_activated_retailers) * 100
                       when kpi_name = "order_frequency_(monthly)" then round(pms.count_of_dns / pms.count_of_customers,1)
                       when kpi_name = "basket_value" then (ps.revenue_in_usd / ps.count_of_dns)
                       when kpi_name = "order_count" then ps.count_of_dns
                       when kpi_name = "cancellations" then (pcd.cancelled_amount_in_usd / pcd.amount_in_usd) * 100
                       when kpi_name = "kyosk_app_(order_count)" then (pwo.count_of_kyosk_app_orders / pwo.count_of_orders) * 100
                       when kpi_name = "kyosk_app_(order_value)" then (pwo.kyosk_app_revenue_in_usd / pwo.ordered_amount_in_usd) * 100
                    else null end as previous_week_actuals,

                    case 
                       when kpi_name = "revenue_(usd)" then cs.revenue_in_usd
                       when kpi_name = "projected_revenue" then cs.projected_revenue_in_usd
                       when kpi_name = "revenue_growth_(%)" then (cs.projected_revenue_in_usd / ps.revenue_in_usd - 1) * 100
                       when kpi_name = "revenue_per_territory" then (cs.revenue_in_usd / cs.count_of_territories)
                       when kpi_name = "front_margin_(%)" then (cwgm.gross_margin_in_usd / cwgm.base_net_amount_in_usd) * 100
                       --when kpi_name = "registered_retailers" then crc.current_count_of_registered_customers
                       when kpi_name = "activated_retailers" then cwar.count_of_activated_retailers
                       when kpi_name = "active_outlets_(monthly)" then cmac.count_of_customers
                       when kpi_name = "activity_rate_(monthly)" then (cmac.count_of_customers / cwar.count_of_activated_retailers) * 100
                       when kpi_name = "active_outlets_(weekly)" then cs.count_of_customers
                       when kpi_name = "activity_rate_(weekly)" then (cs.count_of_customers / cwar.count_of_activated_retailers) * 100
                       when kpi_name = "order_frequency_(monthly)" then round(cms.count_of_dns / cms.count_of_customers,1)
                       when kpi_name = "basket_value" then (cs.revenue_in_usd / cs.count_of_dns)
                       when kpi_name = "order_count" then cs.count_of_dns
                       when kpi_name = "cancellations" then (ccd.cancelled_amount_in_usd / ccd.amount_in_usd) * 100
                       when kpi_name = "kyosk_app_(order_count)" then (cwo.count_of_kyosk_app_orders / cwo.count_of_orders) * 100
                       when kpi_name = "kyosk_app_(order_value)" then (cwo.kyosk_app_revenue_in_usd / cwo.ordered_amount_in_usd) * 100
                    else null end as current_week_actuals,

                    from commercial_kpis ck 

                    left join previous_previous_week_sales ppws on ck.fx_rate_dfn = ppws.fx_rate_dfn
                    
                    left join previous_week_targets pwt on ck.fx_rate_dfn = pwt.fx_rate_dfn
                    left join previous_month_targets pmt on ck.fx_rate_dfn = pmt.fx_rate_dfn

                    left join previous_week_sales ps on ck.fx_rate_dfn = ps.fx_rate_dfn
                    left join previous_week_gross_margin pwgm on ck.fx_rate_dfn = pwgm.fx_rate_dfn
                    left join previous_registered_customers prc on ck.fx_rate_dfn = prc.fx_rate_dfn
                    left join previous_week_activated_retailers pwar on ck.fx_rate_dfn = pwar.fx_rate_dfn
                    left join previous_month_active_customers pmac on ck.fx_rate_dfn = pmac.fx_rate_dfn
                    left join previous_month_registered_customers pmrc on ck.fx_rate_dfn = pmrc.fx_rate_dfn
                    left join previous_week_orders pwo on ck.fx_rate_dfn = pwo.fx_rate_dfn
                    left join previous_cancelled_dns pcd on ck.fx_rate_dfn = pcd.fx_rate_dfn
                    left join previous_month_sales pms on ck.fx_rate_dfn = pms.fx_rate_dfn
                    left join previous_month_orders pmo on ck.fx_rate_dfn = pmo.fx_rate_dfn

                    left join current_week_targets cwt on ck.fx_rate_dfn = cwt.fx_rate_dfn
                    left join current_week_targets_for_projected_rev cwtpr on ck.fx_rate_dfn = cwtpr.fx_rate_dfn
                    left join current_month_targets cmt on ck.fx_rate_dfn = cmt.fx_rate_dfn

                    left join current_week_sales cs on ck.fx_rate_dfn = cs.fx_rate_dfn
                    left join current_week_gross_margin cwgm on ck.fx_rate_dfn = cwgm.fx_rate_dfn
                    left join current_registered_customers crc on ck.fx_rate_dfn = crc.fx_rate_dfn
                    left join current_week_activated_retailers cwar on ck.fx_rate_dfn = cwar.fx_rate_dfn
                    left join current_month_active_customers cmac on ck.fx_rate_dfn = cmac.fx_rate_dfn
                    left join current_month_registered_customers cmrc on ck.fx_rate_dfn = cmrc.fx_rate_dfn
                    left join current_week_orders cwo on ck.fx_rate_dfn = cwo.fx_rate_dfn
                    left join current_cancelled_dns ccd on ck.fx_rate_dfn = ccd.fx_rate_dfn
                    left join current_month_sales cms on ck.fx_rate_dfn = cms.fx_rate_dfn
                    left join current_month_orders cmo on ck.fx_rate_dfn = cmo.fx_rate_dfn

                    left join month_to_date_targets mtdt on ck.fx_rate_dfn = mtdt.fx_rate_dfn
                    left join month_to_date_targets_for_projected_rev mtdtpr on ck.fx_rate_dfn = mtdtpr.fx_rate_dfn

                    left join previous_month_to_date_sales pmtds on ck.fx_rate_dfn = pmtds.fx_rate_dfn

                    left join month_to_date_sales mtds on ck.fx_rate_dfn = mtds.fx_rate_dfn
                    left join month_to_date_gross_margin mtdgm on ck.fx_rate_dfn = mtdgm.fx_rate_dfn
                    left join month_to_date_registered_customers mtdrc on ck.fx_rate_dfn = mtdrc.fx_rate_dfn
                    left join month_to_date_activated_retailers mtdar on ck.fx_rate_dfn = mtdar.fx_rate_dfn
                    left join month_to_date_orders mtdo on ck.fx_rate_dfn = mtdo.fx_rate_dfn
                    left join month_to_date_cancelled_dns mtdcd on ck.fx_rate_dfn = mtdcd.fx_rate_dfn
                   
                  )
select * from final_model order by 1
--select * from month_to_date_targets