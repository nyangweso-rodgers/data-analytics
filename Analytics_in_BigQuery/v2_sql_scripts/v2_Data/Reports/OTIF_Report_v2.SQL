WITH
regional_mapping as (SELECT * FROM `kyosk-prod.erp_reports.upload_regional_mapping`),
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            --and name in ('DN-AJLA-TVXU', 'DN-MAJM-ACKV', 'DN-KISI-UQL5', 'DN-ATHI-AQFE')
                            ),
sales_order_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_order`
                            ),

delivery_note as (
                    select distinct dn.scheduled_delivery_date,
                    dn.posting_date,
                    format_time('%R', dn.posting_time) as posting_time,
                    dn.territory,
                    dn.company,
                    dn.name as delivery_note,
                    kyosk_sales_order as sales_order,
                    dn.lr_no,
                    driver_name,
                    customer,
                    grand_total as grand_total_of_delivery_note,
                    rm.country
                    FROM delivery_note_with_index dn
                    left join regional_mapping rm on dn.territory = rm.territory
                    where index = 1     
                    ),
sales_order as (
                  select distinct so.name as sales_order,
                  format_time('%R', delivery_window_start_time)  as delivery_window_start_time,
                  format_time('%R', delivery_window_end_time)  as delivery_window_end_time,
                  grand_total as grand_total_of_sales_order,
                  from sales_order_with_index so
                  where index = 1
                  ),
      
otif_raw_data as (
                  select dn.*,
                  so.*except(sales_order),
                  case 
                      when (posting_date = scheduled_delivery_date) and (posting_time between delivery_window_start_time and delivery_window_end_time) then 'ON-TIME' 
                      when (posting_date > scheduled_delivery_date) or (posting_date = scheduled_delivery_date and posting_time > delivery_window_end_time) then 'LATE'
                      when (posting_date < scheduled_delivery_date) or (posting_date = scheduled_delivery_date and posting_time < delivery_window_start_time)  then 'EARLY'
                      else null 
                  end as otif_status
                  from delivery_note dn
                  left join sales_order so on dn.sales_order = so.sales_order
                  )
select ord.* from otif_raw_data ord
where FORMAT_DATE('%Y%m%d', scheduled_delivery_date) between @DS_START_DATE and @DS_END_DATE  