----------------------- stock count ---------------
with
------------- uploaded regional mapping table ------------------------
regional_mapping as (
                    select distinct
                    company,
                    original_territory_id,
                    new_territory_id, 
                    from `kyosk-prod.karuru_upload_tables.territory_region_mapping` 
                    ),
-------------------- stock count table ---------------
stock_count as (
                SELECT * 
                FROM `kyosk-prod.karuru_reports.stock_count` 
                WHERE date(created_at) > "2024-04-24" # dataset start date
                ),
stock_count_cte as (
                    select distinct date_trunc(date(created_at), month) as creation_month,
                    created_at,
                    case 
                      when rm.company in ('KYOSK DIGITAL SERVICES LTD (KE)', 'KYOSK DIGITAL SERVICES LIMITED (UG)', 'KYOSK DIGITAL SERVICES LIMITED (TZ)') then date_add(created_at, interval 3 hour)
                      when rm.company in ('KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED') then date_add(created_at, interval 2 hour)
                    else sc.created_at end as created_at_local_datetime,
                    stock_take_event_date,
                    stock_count_completed_at,
                    case 
                      when rm.company in ('KYOSK DIGITAL SERVICES LTD (KE)', 'KYOSK DIGITAL SERVICES LIMITED (UG)', 'KYOSK DIGITAL SERVICES LIMITED (TZ)') then date_add(sc.stock_count_completed_at, interval 3 hour)
                      when rm.company in ('KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED') then date_add(sc.stock_count_completed_at, interval 2 hour)
                    else sc.stock_count_completed_at end as stock_count_completed_at_local_datetime,
                    sc.variance_created_at,
                    sc.variance_explained_at,

                    sc.stock_list_code,
                    rm.company,
                    fulfilment_center_id,
                    fulfilment_center.name as fulfilment_center_name,
                    fulfilment_center.primary_territory_id as fulfilment_center_primary_territory_id,

                    id,
                    stock_take_event_code,
                    case
                      when (stock_take_event_code like "KE-%") or (stock_take_event_code like "UG-%") or (stock_take_event_code like "NG-%") or (stock_take_event_code like "NG-%")   then 'MONTHLY'
                    else 'DAILY' end as stock_take_event_code_type,
                    item_code,
                    erpnext_snapshot_quantity,
                    kyosk_portal_snapshot_quantity,
                    physical_count,
                    erpnext_variance,
                    kyosk_portal_variance,
                    valuation_rate,
                    erpnext_variance_value,
                    kyosk_portal_variance_value,
                    variance_explanation,
                    explained_by,
                    erpnext_snapshot_quantity * valuation_rate as total_value_inventory,
                    physical_count * valuation_rate as total_value_of_physical_inventory
                    from stock_count sc
                    left join regional_mapping rm on sc.fulfilment_center.primary_territory_id = rm.original_territory_id
                    ),
stock_count_with_timestamps_cte as(
                                    select *,
                                    first_value(created_at_local_datetime IGNORE NULLS)over(partition by creation_month, fulfilment_center_name 
                                      order by created_at_local_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as stock_count_territory_start_local_datetime,
                                    last_value(stock_count_completed_at_local_datetime IGNORE NULLS)over(partition by creation_month, fulfilment_center_name 
                                      order by stock_count_completed_at asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as stock_count_territory_completed_local_datetime
                                    from stock_count_cte
                                    ),
stock_count_report_cte as (
                            select *,
                            date_diff(stock_count_territory_completed_local_datetime, stock_count_territory_start_local_datetime, minute) as actual_time_taken_in_minutes
                            from stock_count_with_timestamps_cte
                            )
--select min(created_at) as min_created_at, min(stock_take_event_date) as min_stock_take_event_date from stock_count_cte
--select max(created_at) as max_created_at, max(stock_take_event_date) as max_stock_take_event_date from stock_count_cte
select *
from stock_count_report_cte
where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
and fulfilment_center_name = 'Kiambu' and creation_month = '2024-11-01'
--and FORMAT_DATE('%Y%m%d', date(stock_take_event_date)) between @DS_START_DATE and @DS_END_DATE