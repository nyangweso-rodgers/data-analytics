------------------------ Business Review Dashboard ----------------------------
with 
-------------------------------- Date Variables ----------------------------------
dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date),
vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  --SELECT DATE '2022-09-19' as current_start_date, DATE '2022-09-25' as current_end_date ),
date_vars as (
                select *,

                date_trunc(current_start_date, week(MONDAY)) as current_start_week,
                date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day) as current_end_week,

                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 week) as previous_start_week, 
                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 day) as previous_end_week ,

                date_sub(date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day), interval 1 month) as current_start_month,
                date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day) as current_end_month,

                date_sub(date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day), interval 2 month) as previous_start_month,
                date_sub(date_sub(date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day), interval 1 month), interval 1 day) as previous_end_month
                from vars
                ),
--------------------------------- --------------------------------
business_kpis_data as (SELECT * FROM `kyosk-prod.erp_reports.upload_business_kpis` ),
--business_kpis_targets as ()
customer_with_index as (
                        SELECT *,
                        row_number()over(partition by name order by modified desc) as index 
                        FROM `kyosk-prod.erp_reports.customer` 
                        ),
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            ),

------------------------ Previous Week ----------------------------------
previous_retail_universe as ( 
                              select count(distinct name) as previous_count_of_retail_universe 
                              from customer_with_index, date_vars
                              where index = 1 and date(creation) <= previous_end_week
                              ),
previous_sales as (
                    select  
                         distinct count(distinct dn.kyosk_sales_order) as previous_count_of_orders,
                         count(distinct dn.customer) as previous_count_of_customers,
                         count(distinct (case when dn.created_on_app = 'Duka App' then dn.kyosk_sales_order else null end)) as previous_count_of_duka_app_orders
                    from delivery_note_with_index dn, 
                    date_vars where index = 1 and dn.posting_date between previous_start_week and previous_end_week
                    ),
--------------------------------------- Previous Month ---------------------------------------
previous_month_sales as (
                          select
                              distinct count(distinct dn.customer) as previous_month_count_of_customers
                              from delivery_note_with_index dn, 
                              date_vars where index = 1 and dn.posting_date between previous_start_month and previous_end_month
                              ),

----------------------- Current Week ------------------------------
current_retail_universe as ( 
                              select count(distinct name) as current_count_of_retail_universe 
                              from customer_with_index, date_vars
                              where index = 1 and date(creation) <= current_end_week
                              ),
current_sales as (
                    select 
                         distinct count(distinct dn.kyosk_sales_order) as current_count_of_orders,
                         count(distinct dn.customer) as current_count_of_customers,
                         count(distinct (case when dn.created_on_app = 'Duka App' then dn.kyosk_sales_order else null end)) as current_count_of_duka_app_orders
                    from delivery_note_with_index dn, 
                    date_vars where index = 1 and dn.posting_date between current_start_week and current_end_week
                    ),
------------------------------------ Current Month -----------------------------------
current_month_sales as (
                          select
                              distinct count(distinct dn.customer) as current_month_count_of_customers
                              from delivery_note_with_index dn, 
                              date_vars where index = 1 and dn.posting_date between current_start_month and current_end_month
                              ),

--------------------------------- Final ----------------
final_model as (
                  select bkd.*, '' as blank,


                  case 
                       when kpi_name = 'internal_retail_universe_(registerd_retailers)' then pru.previous_count_of_retail_universe
                       when kpi_name = 'frequent_outlets_(weekly_orders)' then ps.previous_count_of_customers
                       when kpi_name = "active_outlets_(monthly)"  then pms.previous_month_count_of_customers
                       when kpi_name = "internal_retail_universe_activity_(weekly)" then (ps.previous_count_of_customers / pms.previous_month_count_of_customers) * 100
                       when kpi_name = "weekly_order_frequency" then ps.previous_count_of_orders / ps.previous_count_of_customers
                       when kpi_name = 'order_count' then ps.previous_count_of_orders 
                       when kpi_name = "duka_app_adoption_(order_count)" then (ps.previous_count_of_duka_app_orders / ps.previous_count_of_orders) * 100
                  else null end as previous_week_actuals,

                  case 
                       when kpi_name = 'internal_retail_universe_(registerd_retailers)' then cru.current_count_of_retail_universe
                       when kpi_name = 'frequent_outlets_(weekly_orders)' then cs.current_count_of_customers
                       when kpi_name = "active_outlets_(monthly)"  then cms.current_month_count_of_customers
                       when kpi_name = "internal_retail_universe_activity_(weekly)" then (cs.current_count_of_customers / cms.current_month_count_of_customers) * 100
                       when kpi_name = "weekly_order_frequency" then cs. current_count_of_orders / cs.current_count_of_customers
                       when kpi_name = 'order_count' then cs.current_count_of_orders 
                       when kpi_name = "duka_app_adoption_(order_count)" then (cs.current_count_of_duka_app_orders / cs.current_count_of_orders) * 100
                  else null end as current_week_actuals


                  from business_kpis_data bkd,
                  previous_retail_universe pru,
                  previous_sales ps,
                  previous_month_sales pms,

                  current_retail_universe cru, 
                  current_sales cs,
                  current_month_sales cms
                  )
select * from final_model
order by 1