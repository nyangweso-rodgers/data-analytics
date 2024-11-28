-------------------- Cycle Counts - Compliance Report -------------------
with 
/*dates as (
          SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2020-01-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date
          ),*/
vars AS (
  --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE)  as current_end_date ),
  SELECT DATE '2024-11-21' as current_start_date,  DATE '2024-11-21' as current_end_date ),
date_vars_array AS (
                    SELECT date, FORMAT_DATE('%A',date) as day_of_week 
                    FROM vars, UNNEST(GENERATE_DATE_ARRAY(current_start_date, current_end_date, INTERVAL 1 DAY)) AS date
                  ),
date_vars as (  
              select distinct min(date) as current_start_date,
              max(date) as current_end_date,
              count(distinct case when day_of_week <> 'Sunday' then date else null end) as stock_take_event_date_target
              from date_vars_array
                ),
------------- uploaded regional mapping table ------------------------
uploaded_regional_mapping_cte as (
                    select distinct
                    company,
                    original_territory_id,
                    new_territory_id, 
                    from `kyosk-prod.karuru_upload_tables.territory_region_mapping` 
                    ),
stock_count as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.stock_count` 
                WHERE date(created_at) > "2024-04-24" # dataset start date
                and fulfilment_center.primary_territory_id not in ('Karatina', 'Eldoret', 'Juja', 'Kawangware', 'Kisii', 'Meru', 'Ongata Rongai', 'Ruai')
                ),
stock_count_cte as (
                    select distinct stock_take_event_date,
                    case
                      when (stock_take_event_code like "KE-%") or (stock_take_event_code like "UG-%") or (stock_take_event_code like "NG-%") or (stock_take_event_code like "NG-%")   then 'MONTHLY'
                    else 'DAILY' end as stock_take_event_code_type,
                    urm.company,
                    fulfilment_center.primary_territory_id as territory_id 
                    from stock_count sc
                    left join uploaded_regional_mapping_cte urm on sc.fulfilment_center.primary_territory_id = urm.original_territory_id
                    where index =1
                    order by 1,2
                    ),
company_territories_lists_cte as (
                            select distinct company,
                            territory_id,
                            stock_take_event_date_target
                            from stock_count_cte, date_vars
                            where stock_take_event_code_type = 'DAILY'
                            ),
territory_stock_take_event_counts_cte as (
                    select distinct territory_id,
                    --date_vars.sale_days_count,
                    coalesce(count(distinct stock_take_event_date),0) as stock_take_event_date_count,
                    from stock_count_cte, date_vars
                    where stock_take_event_code_type = 'DAILY'
                    and stock_take_event_date between current_start_date and current_end_date
                    group by 1
                    ),
compliance_report as (
                      select ctl.*,
                      coalesce(tstec.stock_take_event_date_count,0) as stock_take_event_date_count
                      from company_territories_lists_cte ctl
                      left join territory_stock_take_event_counts_cte tstec on ctl.territory_id = tstec.territory_id
                      )
select * from compliance_report
--where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
where company = 'KYOSK DIGITAL SERVICES LIMITED (UG)'
--order by company, territory_id