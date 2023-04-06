------------------------------------ QA Script - Business Review Dashboard -------------------------
------------------------------------ Created By: Rodgers ----------------
with

dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date),
daily_sale_days as (select * from dates where FORMAT_DATE('%A',date) <> 'Sunday' ),

dates_2 as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-02-06',last_day(CURRENT_DATE()), INTERVAL 1 DAY)) AS date),
monthly_sale_days as ( select distinct date_trunc(date,month) as month, count(distinct date) as days_in_month from dates_2 group by 1 order by 1),

vars AS (
          --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
          SELECT DATE '2023-03-20' as current_start_date, DATE '2023-03-26' as current_end_date ),
date_vars as (
                select *,

                date_trunc(current_start_date, week(MONDAY)) as current_start_week,
                date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day) as current_end_week,

                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 week) as previous_start_week, 
                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 day) as previous_end_week ,

                date_sub(date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day), interval 1 month) as current_start_month,
                date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day) as current_end_month,

                date_sub(date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day), interval 2 month) as previous_start_month,
                date_sub(date_sub(date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day), interval 1 month), interval 1 day) as previous_end_month,

                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 2 week) as previous_previous_start_week, 
                date_add(date_sub(date_trunc(current_start_date, week(MONDAY)), interval 2 week), interval 6 day) as previous_previous_end_week ,

                case when date_trunc(current_date(),month) = date_trunc(current_end_date, month) then date_trunc(current_end_date, month) else date_trunc(current_start_date, month) end  as month_to_date_start,
                current_date() as month_to_date_end,

                date_sub(case when date_trunc(current_date(),month) = date_trunc(current_end_date, month) then date_trunc(current_end_date, month) else date_trunc(current_start_date, month) end , interval 1 month) as previous_month_to_date_start,
                date_sub(current_date(), interval 1 month) as previous_month_to_date_end,

                case when date_trunc(current_date(),week(MONDAY)) = date_trunc(current_start_date, week(MONDAY)) then date_diff(current_date(),date_trunc(current_start_date, week(MONDAY)) , day) else date_diff(date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day),date_trunc(current_start_date, week(MONDAY)),day) end as current_wtd_days,

                case when date_trunc(current_date(),month) = date_trunc(current_start_date, month) and date_trunc(current_date(),week(MONDAY)) = date_trunc(current_start_date, week(MONDAY)) then date_diff(current_date(),date_trunc(current_start_date, month), day) else date_diff(date_sub(current_end_date, interval 1 day),date_trunc(current_end_date, month)- 1,day) end as mtd_days
              
                from vars
                ),
---------------------------------------------------- KPIs Table -------------------------------
commercial_kpis as (select * from `uploaded_tables.uploaded_table_commercial_kpis_v5` where deliverable = 'Commercial'),

------------------------------ DailY Transactions ----------------------------------
sales_orders as (
                select distinct transaction_date,
                fx_rate_dfn,
                created_on_app,
                sales_order,
                customer,
                grand_total_in_usd
                from `kyosk-prod.erp_scheduled_queries.erp_sales_order_v2`
                ),
deliveries as (
                select distinct posting_date,
                delivery_notes,
                customer,
                grand_total_in_usd,
                fx_rate_dfn
                from `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns_v2`
                ),
----------------------------------- Current Week ------------------------------
current_week_orders as (
                              select  distinct fx_rate_dfn,
                              count(distinct so.customer) as count_of_customers,
                              sum(grand_total_in_usd) as grand_total_in_usd,
                              count(distinct so.sales_order) as count_of_sale_orders,
                              count(distinct (case when created_on_app = 'Duka App' then sales_order else null end)) as count_of_kyosk_app_orders,
                              sum(case when created_on_app = 'Duka App' then grand_total_in_usd else null end) as kyosk_app_revenue_in_usd,
                              from sales_orders so, date_vars where so.transaction_date between current_start_week and current_end_week
                              group by 1
                              ),
current_week_deliveries as (
                              select  distinct fx_rate_dfn,
                              count(distinct d.customer) as count_of_customers,
                              sum(grand_total_in_usd) as grand_total_in_usd,
                              count(distinct d.delivery_notes) as count_of_delivery_notes
                              from deliveries d, date_vars where posting_date between current_start_week and current_end_week
                              group by 1
                              ),
----------------------------------- Previous Week ------------------------------
previous_week_orders as (
                              select  distinct fx_rate_dfn,
                              count(distinct so.customer) as count_of_customers,
                              sum(grand_total_in_usd) as grand_total_in_usd,
                              count(distinct so.sales_order) as count_of_sale_orders,
                              count(distinct (case when created_on_app = 'Duka App' then sales_order else null end)) as count_of_kyosk_app_orders,
                              sum(case when created_on_app = 'Duka App' then grand_total_in_usd else null end) as kyosk_app_revenue_in_usd,
                              from sales_orders so, date_vars where so.transaction_date between previous_start_week and previous_end_week
                              group by 1
                              ),
previous_week_deliveries as (
                              select  distinct fx_rate_dfn,
                              count(distinct d.customer) as count_of_customers,
                              sum(grand_total_in_usd) as grand_total_in_usd,
                              count(distinct d.delivery_notes) as count_of_delivery_notes
                              from deliveries d, date_vars where posting_date between previous_start_week and previous_end_week
                              group by 1
                              ),

------------------------------------------------ Model ------------------------
final_model as (
                select ck.*,
                case
                  when kpi_name = "revenue_(usd)" then pwd.grand_total_in_usd
                  when kpi_name = "active_outlets_(weekly)" then pwd.count_of_customers
                  when kpi_name = "basket_value" then pwd.grand_total_in_usd / pwd.count_of_delivery_notes
                  when kpi_name = "order_count" then pwo.count_of_sale_orders
                else null end as previous_week_actuals,

                case
                  when kpi_name = "revenue_(usd)" then cwd.grand_total_in_usd
                  when kpi_name = "active_outlets_(weekly)" then cwd.count_of_customers
                  when kpi_name = "basket_value" then cwd.grand_total_in_usd / cwd.count_of_delivery_notes
                  when kpi_name = "order_count" then cwo.count_of_sale_orders
                else null end as current_week_actuals
                from commercial_kpis ck

                left join previous_week_orders pwo on ck.fx_rate_dfn = pwo.fx_rate_dfn
                left join previous_week_deliveries pwd on ck.fx_rate_dfn = pwd.fx_rate_dfn

                left join current_week_orders cwo on ck.fx_rate_dfn = cwo.fx_rate_dfn
                left join current_week_deliveries cwd on ck.fx_rate_dfn = cwd.fx_rate_dfn
                --previous_week_deliveries pwd 
                --using (fx_rate_dfn)
                order by 1
                )


select * from final_model