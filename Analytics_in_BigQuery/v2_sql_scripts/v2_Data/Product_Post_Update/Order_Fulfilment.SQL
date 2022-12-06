------------------------------ Weekly Product Post Update - KPIs -----------------------
with 
----------------------------------- Date Variables ----------------------------------------------
dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date),
vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  --SELECT DATE '2022-11-14' as current_start_date, DATE '2022-11-20' as current_end_date ),
date_vars as (
                select *,

                date_trunc(current_start_date, week(MONDAY)) as current_start_week,
                date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day) as current_end_week,

                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 week) as previous_start_week, 
                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 day) as previous_end_week ,

                from vars
                ), 
--------------------------------------- Data --------------------------------------------------
product_kpis as (SELECT * FROM `kyosk-prod.erp_reports.upload_products_post_update_kpis` ),
gross_margin_report as (
                         select distinct posting_date,
                         sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                         sum(total_incoming_rate_in_usd) as total_incoming_rate_in_usd
                         from `kyosk-prod.erp_reports.scheduled_gross_margin_report_v3`
                         --where posting_date >= '2022-11-16' -- Enforcing Partition Field
                         group by 1
                         ),
currency_conversion_data as (SELECT * FROM `kyosk-prod.erp_reports.upload_currency_conversion_v2` where ex_rate not in ('BUDGET RATE (2022)') ),
delivery_trip_with_index as (
                              select *,
                              case 
                                when company = 'KYOSK DIGITAL SERVICES LTD (KE)' then TIMESTAMP_ADD(creation, interval 3 HOUR) 
                                when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then TIMESTAMP_ADD(creation, interval 3 HOUR) 
                                when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then TIMESTAMP_ADD(creation, interval 3 HOUR) 
                                when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then TIMESTAMP_ADD(creation, interval 1 HOUR) 
                              else creation end as creation_time_in_local,

                              case 
                              when company = 'KYOSK DIGITAL SERVICES LTD (KE)' then TIMESTAMP_ADD(dispatched_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then TIMESTAMP_ADD(dispatched_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then TIMESTAMP_ADD(dispatched_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then TIMESTAMP_ADD(dispatched_time, interval 1 HOUR) 
                            else dispatched_time end as dispatched_time_in_local,

                              row_number()over(partition by name order by modified desc) as index,
                              from `erp_reports.delivery_trip`
                              where  workflow_state in ('COMPLETED')
                              --and date(completed_time) = '2022-10-24'
                              --where company in ('KYOSK DIGITAL SERVICES LTD (KE)')
                            ),
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state not in ('SUBMITTED')
                            ),
sales_order_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_order`
                            ),
delivery_note_sales_order_mashup as (
                                      select distinct dn.name as delivery_note,
                                      lr_no as delivery_trip,
                                      dn.scheduled_delivery_date,
                                      dn.posting_date,
                                      dn.posting_time,
                                      format_time('%R', dn.posting_time) as posting_hour_minute,
                                      format_time('%R', delivery_window_start_time)  as delivery_window_start_time,
                                      format_time('%R', delivery_window_end_time)  as delivery_window_end_time,
                                      from delivery_note_with_index dn
                                      left join sales_order_with_index so on dn.kyosk_sales_order = so.name
                                      where dn.index = 1
                                      and so.index = 1
                                      and dn.workflow_state in ('PAID', 'DELIVERED')
                                      ),
otif_raw_data as (
                  select distinct scheduled_delivery_date,
                  delivery_note,
                  case 
                      when (posting_date = scheduled_delivery_date) and (posting_hour_minute between delivery_window_start_time and delivery_window_end_time) then 'ON-TIME' 
                      when (posting_date > scheduled_delivery_date) or (posting_date = scheduled_delivery_date and posting_hour_minute > delivery_window_end_time) then 'LATE'
                      when (posting_date < scheduled_delivery_date) or (posting_date = scheduled_delivery_date and posting_hour_minute < delivery_window_start_time)  then 'EARLY'
                      else null 
                  end as otif_status
                  from delivery_note_sales_order_mashup
                  ),
average_time_between_notes as (
                                select distinct posting_date,
                                delivery_trip,
                                delivery_note,
                                coalesce(TIME_DIFF(posting_time, 
                                          lag(posting_time) over(partition by posting_date,delivery_trip order by posting_date,posting_time), minute),0) as time_variance_in_minutes
                                from delivery_note_sales_order_mashup 
                                ),
trip_generation_to_dispatch_report as (
                                        select distinct name as delivery_trip,
                                        date(creation_time_in_local) as creation_date_in_local,
                                        creation_time_in_local,
                                        dispatched_time_in_local,
                                        DATETIME_DIFF(dispatched_time_in_local, creation_time_in_local, MINUTE) as trip_creation_to_dispatch_in_mins
                                        from delivery_trip_with_index
                                        where index = 1
                                        ),
scheduled_delivery_note as (
                              select distinct dn.scheduled_delivery_date,
                              dn.workflow_state,
                              dn.grand_total,
                              dn.grand_total / ccd.ex_rate_amount as grand_total_in_usd
                              from delivery_note_with_index dn
                              left join currency_conversion_data ccd on dn.company = ccd.company and dn.posting_date between ccd.start_date and ccd.end_date
                              where index = 1
                              ),
---------------------------------------------- Previous Week -------------------------------------------
previous_gross_margin_report as (
                                   select distinct sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                                   sum(base_net_amount_in_usd - total_incoming_rate_in_usd) as gross_margin_in_usd,
                                   from gross_margin_report gmr,
                                   date_vars where gmr.posting_date between previous_start_week and previous_end_week
                                   ),
previous_otif as (
                    select distinct count(distinct delivery_note) as count_of_delivery_notes,
                    count(distinct (case when otif_status = 'ON-TIME' then delivery_note else null end)) as count_of_on_time_delivery_notes 
                    from otif_raw_data, date_vars where scheduled_delivery_date between previous_start_week and previous_end_week
                    ),
previous_average_time_between_notes as (
                                          select distinct count(distinct delivery_note) as count_of_delivery_notes,
                                          sum(time_variance_in_minutes) as time_variance_in_minutes
                                          from average_time_between_notes,  date_vars where posting_date between previous_start_week and previous_end_week
                                          ),
previous_trip_generation_to_dispatch_report as (
                                                select distinct
                                                avg(trip_creation_to_dispatch_in_mins) as trip_creation_to_dispatch_in_mins
                                                from trip_generation_to_dispatch_report, date_vars where creation_date_in_local between previous_start_week and previous_end_week
                                                ),
previous_note_cancellations as (
                                select distinct 
                                sum(grand_total_in_usd) as grand_total_in_usd,
                                sum(case when workflow_state = 'CANCELLED' THEN grand_total_in_usd else null end) as cancelled_grand_total_in_usd
                                from scheduled_delivery_note, date_vars where scheduled_delivery_date between previous_start_week and previous_end_week
                                ),
-------------------------------------------- Current Week ----------------------------------------------
current_gross_margin_report as (
                                   select distinct sum(base_net_amount_in_usd) as base_net_amount_in_usd,
                                   sum(base_net_amount_in_usd - total_incoming_rate_in_usd) as gross_margin_in_usd,
                                   from gross_margin_report gmr,
                                   date_vars where gmr.posting_date between current_start_week and current_end_week
                                   ),
current_otif as (
                    select distinct count(distinct delivery_note) as count_of_delivery_notes,
                    count(distinct (case when otif_status = 'ON-TIME' then delivery_note else null end)) as count_of_on_time_delivery_notes 
                    from otif_raw_data, date_vars where scheduled_delivery_date between current_start_week and current_end_week
                    ),
current_average_time_between_notes as (
                                          select distinct count(distinct delivery_note) as count_of_delivery_notes,
                                          sum(time_variance_in_minutes) as time_variance_in_minutes
                                          from average_time_between_notes,  date_vars where posting_date between current_start_week and current_end_week
                                          ),
current_trip_generation_to_dispatch_report as (
                                                select distinct avg(trip_creation_to_dispatch_in_mins) as trip_creation_to_dispatch_in_mins
                                                from trip_generation_to_dispatch_report, date_vars where creation_date_in_local between current_start_week and current_end_week
                                                ),
current_note_cancellations as (
                                select distinct 
                                sum(grand_total_in_usd) as grand_total_in_usd,
                                sum(case when workflow_state = 'CANCELLED' THEN grand_total_in_usd else null end) as cancelled_grand_total_in_usd
                                from scheduled_delivery_note, date_vars where scheduled_delivery_date between current_start_week and current_end_week
                                ),
------------------------------- Final Mode -----------------------------------
final_model as (
                  select k.*,

                  case 
                      when kpi_name = "profitability:_overall_target_margin_for_fmcg_sales" then (pgm.gross_margin_in_usd / pgm.base_net_amount_in_usd) * 100
                      when kpi_name = "dispatch_optimization:_note_cancellation_rate" then (pnc.cancelled_grand_total_in_usd / pnc.grand_total_in_usd) * 100
                      when kpi_name = "dispatch_optimization:_on_time_in_full_(otif)_delivery_to_retailers" then (po.count_of_on_time_delivery_notes / po.count_of_delivery_notes) * 100 
                      when kpi_name = "dispatch_optimization:_order_processing_(trip_generation)_to_dispatch_time" then ptgtdr.trip_creation_to_dispatch_in_mins
                      when kpi_name = "delivery_optimization:_average_time_between_deliveries" then pvtbn.time_variance_in_minutes / pvtbn.count_of_delivery_notes
                  else null end as previous_week_actuals,

                  case 
                      when kpi_name = "profitability:_overall_target_margin_for_fmcg_sales" then (cgm.gross_margin_in_usd / cgm.base_net_amount_in_usd) * 100
                      when kpi_name = "dispatch_optimization:_note_cancellation_rate" then (cnc.cancelled_grand_total_in_usd / cnc.grand_total_in_usd) * 100
                      when kpi_name = "dispatch_optimization:_on_time_in_full_(otif)_delivery_to_retailers" then (co.count_of_on_time_delivery_notes / co.count_of_delivery_notes) * 100
                      when kpi_name = "dispatch_optimization:_order_processing_(trip_generation)_to_dispatch_time" then ctgtdr.trip_creation_to_dispatch_in_mins
                      when kpi_name = "delivery_optimization:_average_time_between_deliveries" then catbn.time_variance_in_minutes / catbn.count_of_delivery_notes
                  else null end as current_week_actuals


                  from product_kpis k,

                  previous_gross_margin_report pgm,
                  previous_otif po,
                  previous_average_time_between_notes as pvtbn,
                  previous_trip_generation_to_dispatch_report ptgtdr,
                  previous_note_cancellations pnc,

                  current_gross_margin_report cgm,
                  current_otif co,
                  current_average_time_between_notes catbn,
                  current_trip_generation_to_dispatch_report ctgtdr,
                  current_note_cancellations cnc
                  )
select * from final_model
order by 1