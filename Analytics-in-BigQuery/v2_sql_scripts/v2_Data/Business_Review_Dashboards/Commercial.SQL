------------------------ Business Review Dashboard - Commercial ----------------------------
with 
-------------------------------- Date Variables ----------------------------------
dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date),
daily_sale_days as (select * from dates where FORMAT_DATE('%A',date) <> 'Sunday' ),

vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  --SELECT DATE '2022-11-21' as current_start_date, DATE '2022-11-27' as current_end_date ),
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
                from vars
                ),
--------------------------------- --------------------------------
currency_conversion_data as (SELECT * FROM `kyosk-prod.erp_reports.upload_currency_conversion_v2` where ex_rate not in ('BUDGET RATE (2022)') ),
business_kpis_data as (SELECT * FROM `kyosk-prod.erp_reports.upload_business_kpis` where deliverable = 'Commercial'),
targets as (SELECT * FROM `kyosk-prod.erp_reports.upload_business_kpis_targets` ),
targets_with_daily_sales_dates as (select * from targets, daily_sale_days),
gross_margin_report as (
                         select distinct posting_date,
                         sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                         sum(total_incoming_rate_in_usd) as total_incoming_rate_in_usd
                         from `kyosk-prod.erp_reports.scheduled_gross_margin_report_v3`
                         --where posting_date >= '2022-11-16' -- Enforcing Partition Field
                         group by 1
                         ),
customer_with_index as ( SELECT *, row_number()over(partition by name order by modified desc) as index FROM `kyosk-prod.erp_reports.customer` ),
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state not in ('SUBMITTED')
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            ),
delivery_note_report as (
                            select distinct dnwi.posting_date,
                            dnwi.customer,
                            dnwi.kyosk_sales_order as sales_order,
                            dnwi.created_on_app,
                            dnwi.grand_total,
                            dnwi.grand_total / ccd.ex_rate_amount as grand_total_in_usd
                            from delivery_note_with_index dnwi
                            left join currency_conversion_data ccd on dnwi.company = ccd.company and dnwi.posting_date between ccd.start_date and ccd.end_date      
                            where index = 1
                            and workflow_state in ('PAID', 'DELIVERED')
                            ),
scheduled_delivery_note as (
                              select distinct dn.scheduled_delivery_date,
                              dn.name as delivery_note,
                              dn.workflow_state,
                              odni.fulfilment_status,
                              odni.name,
                              odni.base_amount,
                              odni.base_amount / ccd.ex_rate_amount as base_amount_in_usd
                              from delivery_note_with_index dn,unnest(original_items) odni
                              left join currency_conversion_data ccd on dn.company = ccd.company and dn.posting_date between ccd.start_date and ccd.end_date
                              where index = 1
                              ),
---------------------------------------------- Previous Previous Week --------------------------------------
previous_previous_sales as (
                              select  distinct sum(grand_total_in_usd) as revenue_in_usd
                              from delivery_note_report dn, 
                              date_vars where dn.posting_date between previous_previous_start_week and previous_previous_end_week
                              ),
------------------------ Previous Week ----------------------------------
previous_week_targets as (
                      select distinct 
                      sum(revenue_in_usd) as revenue_in_usd,
                      avg(margin) as margin,
                      sum(internal_retail_universe_registerd_retailers) as internal_retail_universe_registerd_retailers,
                      avg(frequent_outlets_weekly_orders) as frequent_outlets_weekly_orders,
                      avg(active_outlets_monthly) as active_outlets_monthly,
                      avg(internal_retail_universe_activity_weekly) as internal_retail_universe_activity_weekly,
                      avg(weekly_order_frequency) as weekly_order_frequency,
                      avg(cancellations) as cancellations,
                      avg(duka_app_adoption_order_count) as duka_app_adoption_order_count,
                      avg(duka_app_adoption_order_value) as duka_app_adoption_order_value,
                      avg(cash_flow_days_inventory_outstanding) as cash_flow_days_inventory_outstanding,
                      sum(market_development_cost) as market_development_cost
                      from targets_with_daily_sales_dates, date_vars
                      where (date between start_date and end_date) and (date between date_vars.previous_start_week and date_vars.previous_end_week)
                      ),
previous_retail_universe as ( 
                              select count(distinct name) as previous_count_of_retail_universe 
                              from customer_with_index, date_vars
                              where index = 1 and date(creation) <= previous_end_week
                              ),
previous_sales as (
                    select  
                         distinct count(distinct dn.sales_order) as count_of_orders,
                         count(distinct dn.customer) as count_of_customers,
                         count(distinct (case when dn.created_on_app = 'Duka App' then dn.sales_order else null end)) as count_of_duka_app_orders,
                         sum(case when dn.created_on_app = 'Duka App' then dn.grand_total_in_usd else null end) as duka_app_revenue_in_usd,
                         sum(grand_total_in_usd) as revenue_in_usd
                    from delivery_note_report dn, 
                    date_vars where dn.posting_date between previous_start_week and previous_end_week
                    ),
previous_scheduled_deliveries as (
                                   select distinct sum(base_amount_in_usd) as base_amount_in_usd,
                                   sum(case when fulfilment_status = 'CANCELLED' then base_amount_in_usd else null end) as cancelled_amount_in_usd
                                   from scheduled_delivery_note sdn,
                                   date_vars where sdn.scheduled_delivery_date between previous_start_week and previous_end_week
                                   ),
previous_gross_margin_report as (
                                   select distinct sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                                   sum(base_net_amount_in_usd - total_incoming_rate_in_usd) as gross_margin_in_usd,
                                   from gross_margin_report gmr,
                                   date_vars where gmr.posting_date between previous_start_week and previous_end_week
                                   ),
--------------------------------------- Previous Month ---------------------------------------
previous_month_sales as (
                          select distinct dn.customer, count(distinct dn.posting_date) as count_of_posting_dates
                          from delivery_note_report dn, 
                          date_vars where dn.posting_date between previous_start_month and previous_end_month
                          group by 1
                         ),
previous_month_active_customers as (select distinct count(distinct customer) as count_of_customers from previous_month_sales),
previous_month_frequent_customers as (select distinct count(distinct customer) as count_of_customers from previous_month_sales where count_of_posting_dates >= 4),

----------------------- Current Week ------------------------------
current_week_targets as (
                      select distinct 
                      sum(revenue_in_usd) as revenue_in_usd,
                      avg(revenue_growth) as revenue_growth,
                      avg(margin) as margin,
                      sum(internal_retail_universe_registerd_retailers) as internal_retail_universe_registerd_retailers,
                      avg(frequent_outlets_weekly_orders) as frequent_outlets_weekly_orders,
                      avg(active_outlets_monthly) as active_outlets_monthly,
                      avg(internal_retail_universe_activity_weekly) as internal_retail_universe_activity_weekly,
                      avg(weekly_order_frequency) as weekly_order_frequency,
                      avg(basket_size) as basket_size,
                      sum(order_count) as order_count,
                      avg(cancellations) as cancellations,
                      avg(duka_app_adoption_order_count) as duka_app_adoption_order_count,
                      avg(duka_app_adoption_order_value) as duka_app_adoption_order_value,
                      avg(cash_flow_days_inventory_outstanding) as cash_flow_days_inventory_outstanding,
                      sum(market_development_cost) as market_development_cost
                      from targets_with_daily_sales_dates, date_vars
                      where (date between start_date and end_date) and (date between date_vars.current_start_week and date_vars.current_end_week)
                      ),
current_retail_universe as ( 
                              select count(distinct name) as current_count_of_retail_universe 
                              from customer_with_index, date_vars
                              where index = 1 and date(creation) <= current_end_week
                              ),
current_sales as (
                    select 
                         distinct count(distinct dn.sales_order) as count_of_orders,
                         count(distinct dn.customer) as count_of_customers,
                         count(distinct (case when dn.created_on_app = 'Duka App' then dn.sales_order else null end)) as count_of_duka_app_orders,
                         sum(case when dn.created_on_app = 'Duka App' then dn.grand_total_in_usd else null end) as duka_app_revenue_in_usd,
                         sum(grand_total_in_usd) as revenue_in_usd
                    from delivery_note_report dn, 
                    date_vars where dn.posting_date between current_start_week and current_end_week
                    ),
current_scheduled_deliveries as (
                                   select distinct sum(base_amount_in_usd) as base_amount_in_usd,
                                   sum(case when fulfilment_status = 'CANCELLED' then base_amount_in_usd else null end) as cancelled_amount_in_usd
                                   from scheduled_delivery_note sdn,
                                   date_vars where sdn.scheduled_delivery_date between current_start_week and current_end_week
                                   ),
current_gross_margin_report as (
                                   select distinct sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                                   sum(base_net_amount_in_usd - total_incoming_rate_in_usd) as gross_margin_in_usd,
                                   from gross_margin_report gmr,
                                   date_vars where gmr.posting_date between current_start_week and current_end_week
                                   ),
------------------------------------ Current Month -----------------------------------
current_month_sales as (
                          select distinct dn.customer,
                          count(distinct dn.posting_date) as count_of_posting_dates
                          from delivery_note_report dn, 
                          date_vars where dn.posting_date between current_start_month and current_end_month
                          group by 1
                         ),
current_month_active_customers as (select distinct count(distinct customer) as count_of_customers from current_month_sales),
current_month_frequent_customers as (select distinct count(distinct customer) as count_of_customers from current_month_sales where count_of_posting_dates >= 4),

--------------------------------- Final ----------------
final_model as (
                  select bkd.*, 
                  '' as blank,

                  case
                    when kpi_name = "revenue_(usd)" then pwt.revenue_in_usd
                    when kpi_name = "margin_(%)" then pwt.margin
                    when kpi_name = "cancellations" then pwt.cancellations
                    when kpi_name = "duka_app_adoption_(order_count)" then pwt.duka_app_adoption_order_count
                    when kpi_name = "duka_app_adoption_(order_value)" then pwt.duka_app_adoption_order_value
                    when kpi_name = "cash_flow_(days_inventory_outstanding)" then pwt.cash_flow_days_inventory_outstanding
                  else null end as previous_week_targets,

                  case
                    when kpi_name = "revenue_(usd)" then cwt.revenue_in_usd
                    when kpi_name = "margin_(%)" then cwt.margin
                    when kpi_name = "cancellations" then cwt.cancellations
                    when kpi_name = "duka_app_adoption_(order_count)" then cwt.duka_app_adoption_order_count
                    when kpi_name = "duka_app_adoption_(order_value)" then cwt.duka_app_adoption_order_value
                    when kpi_name = "cash_flow_(days_inventory_outstanding)" then cwt.cash_flow_days_inventory_outstanding
                  else null end as current_week_targets,



                  case 
                       when kpi_name = "revenue_(usd)" then ps.revenue_in_usd
                       when kpi_name = "revenue_growth_(%)" then (ps.revenue_in_usd / pps.revenue_in_usd - 1) * 100
                       when kpi_name = "margin_(%)" then (pgmr.gross_margin_in_usd / pgmr.base_net_amount_in_usd) * 100
                       when kpi_name = 'internal_retail_universe_(registerd_retailers)' then pru.previous_count_of_retail_universe
                       when kpi_name = "active_outlets_(monthly)"  then pmac.count_of_customers
                       when kpi_name =  "active_outlets_(weekly)" then ps.count_of_customers
                       when kpi_name = "frequent_outlets_(weekly_orders_in_the_past_month)" then pmfc.count_of_customers
                       when kpi_name = "internal_retail_universe_activity_(weekly)" then (ps.count_of_customers / pmac.count_of_customers) * 100
                       when kpi_name = "weekly_order_frequency" then round(ps.count_of_orders / ps.count_of_customers,1)
                       when kpi_name = "basket_size" then ps.revenue_in_usd / ps.count_of_orders
                       when kpi_name = 'order_count' then ps.count_of_orders 
                       when kpi_name = "cancellations" then (psd.cancelled_amount_in_usd / psd.base_amount_in_usd) * 100
                       when kpi_name = "duka_app_adoption_(order_count)" then (ps.count_of_duka_app_orders / ps.count_of_orders) * 100
                       when kpi_name = "duka_app_adoption_(order_value)" then (ps.duka_app_revenue_in_usd / ps.revenue_in_usd) * 100
                  else null end as previous_week_actuals,

                  case 
                       when kpi_name = "revenue_(usd)" then cs.revenue_in_usd
                       when kpi_name = "revenue_growth_(%)" then (cs.revenue_in_usd / ps.revenue_in_usd -1) * 100
                       when kpi_name = "margin_(%)" then (cgmr.gross_margin_in_usd / cgmr.base_net_amount_in_usd) * 100
                       when kpi_name = 'internal_retail_universe_(registerd_retailers)' then cru.current_count_of_retail_universe
                       when kpi_name = "active_outlets_(monthly)"  then cmac.count_of_customers
                       when kpi_name =  "active_outlets_(weekly)" then cs.count_of_customers
                       when kpi_name = "frequent_outlets_(weekly_orders_in_the_past_month)" then cmfc.count_of_customers
                       when kpi_name = "internal_retail_universe_activity_(weekly)" then (cs.count_of_customers / cmac.count_of_customers) * 100
                       when kpi_name = "weekly_order_frequency" then round(cs.count_of_orders / cs.count_of_customers,1)
                       when kpi_name = "basket_size" then cs.revenue_in_usd / cs.count_of_orders
                       when kpi_name = 'order_count' then cs.count_of_orders 
                       when kpi_name = "cancellations" then (csd.cancelled_amount_in_usd / csd.base_amount_in_usd) * 100
                       when kpi_name = "duka_app_adoption_(order_count)" then (cs.count_of_duka_app_orders / cs.count_of_orders) * 100
                       when kpi_name = "duka_app_adoption_(order_value)" then (cs.duka_app_revenue_in_usd / cs.revenue_in_usd) * 100
                  else null end as current_week_actuals


                  from business_kpis_data bkd,
                  previous_month_active_customers pmac,
                  previous_month_frequent_customers pmfc,

                  previous_previous_sales pps,

                  previous_week_targets pwt,
                  previous_retail_universe pru,
                  previous_sales ps,
                  previous_scheduled_deliveries psd,
                  previous_gross_margin_report pgmr,

                  current_week_targets cwt,
                  current_retail_universe cru, 
                  current_sales cs,
                  current_scheduled_deliveries csd,
                  current_gross_margin_report cgmr,

                  current_month_active_customers cmac,
                  current_month_frequent_customers cmfc
                  )
select * from final_model
order by 1