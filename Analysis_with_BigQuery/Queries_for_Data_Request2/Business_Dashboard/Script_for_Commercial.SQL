------------------------ Business Review Dashboard ----------------------------
with 
-------------------------------- Date Variables ----------------------------------
dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date),
vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  --SELECT DATE '2022-09-23' as current_start_date, DATE '2022-09-24' as current_end_date ),
date_vars as (
                select *,
                date_trunc(current_start_date, week) as current_start_week,
                date_add(date_trunc(current_start_date, week), interval 6 day) as current_end_week,
                date_sub(date_trunc(current_start_date, week), interval 1 week) as previous_start_week, 
                date_sub(date_trunc(current_end_date, week), interval 1 day) as previous_end_week 
                from vars
                ),
--------------------------------- --------------------------------
business_kpis_data as (SELECT * FROM `kyosk-prod.erp_reports.upload_business_kpis` ),
--business_kpis_targets as ()
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            ),

------------------------ Previous Week ----------------------------------
previous_order_count as (
                          select distinct count(distinct dn.kyosk_sales_order) as previous_count_of_orders,
                          count(distinct dn.customer) as previous_count_of_customers
                          from delivery_note_with_index dn, 
                          date_vars where index = 1 and dn.posting_date between previous_start_week and previous_end_week
                          ),

----------------------- Current Week ------------------------------
current_order_count as (
                          select distinct count(distinct dn.kyosk_sales_order) as current_count_of_orders,
                          count(distinct dn.customer) as current_count_of_customers
                          from delivery_note_with_index dn, 
                          date_vars where index = 1 and dn.posting_date between current_start_week and current_end_week
                          ),

--------------------------------- Final ----------------
final_model as (
                  select bkd.*, '' as blank,
                  case when kpi_name = 'frequent_outlets_(weekly_orders)' then poc.previous_count_of_customers
                       when kpi_name = 'order_count' then poc.previous_count_of_orders 
                  else null end as previous_week_actuals,

                  case when kpi_name = 'frequent_outlets_(weekly_orders)' then coc.current_count_of_customers
                       when kpi_name = 'order_count' then coc.current_count_of_orders 
                  else null end as current_week_actuals


                  from business_kpis_data bkd,
                  previous_order_count poc,
                  current_order_count coc
                  )
select * from final_model
order by 1