----------------- Sales Invoice -------------------
with
---------------------- Dynamic Date Variables for SQL Scripts ---------------------- 
dates as (
          SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2020-01-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date
          ),
vars AS (
 --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) end  as current_end_date ),
  SELECT DATE '2024-08-01' as current_start_date,  DATE '2021-08-31' as current_end_date ),

date_vars as (  
              select *,
                date_trunc(current_start_date, month) as current_start_month,
                last_day(date_trunc(current_start_date, month)) as current_end_month,
                date_sub(date_trunc(current_start_date, month), interval 1 month) as previous_start_month,
                date_sub(date_trunc(current_start_date, month), interval 1 day) as previous_end_month,
              from vars
              ),
--------------------- Delivery Trips -------------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date(created_at) = current_date
                where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                --where date(created_at) between '2023-08-01' and '2024-01-23'
                --and is_pre_karuru = false
              ),
----------------- Scheduled front margins ----------------------------
front_margins_report as (
                        SELECT distinct delivery_date,
                        company,
                        territory_id,
                        outlet_id,
                        kyosk_delivery_note,
                        --delivery_note,
                        item_code,
                        --uom,
                        sum(base_net_amount) as base_net_amount,
                        sum(total_incoming_rate) as total_incoming_rate
                        FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                        wHERE delivery_date > date_sub(date_trunc(current_date, month), interval 3 month)
                        group by 1,2,3,4,5,6
                        ),
current_month_front_margins as (
                                  select distinct  date_trunc(fmr.delivery_date, month) as current_delivery_month,
                                  company,
                                  territory_id,
                                  item_code,
                                  outlet_id,
                                  kyosk_delivery_note,
                                  sum(base_net_amount) as gmv_vat_excl,
                                  sum(total_incoming_rate) as avg_cost_vat_excl
                                  from front_margins_report fmr, date_vars
                                  where delivery_date between current_start_month and current_end_month
                                  group by 1,2,3,4,5,6
                                  ),
previous_month_front_margins as (
                                  select distinct  date_trunc(fmr.delivery_date, month) as previous_delivery_month,
                                  territory_id,
                                  item_code,
                                  outlet_id,
                                  kyosk_delivery_note,
                                  sum(base_net_amount) as gmv_vat_excl,
                                  sum(total_incoming_rate) as avg_cost_vat_excl
                                  from front_margins_report fmr, date_vars
                                  where delivery_date between previous_start_month and previous_end_month
                                  group by 1,2,3,4,5
                                  )
select  distinct cmfm.current_delivery_month, 
pmfm.previous_delivery_month,
cmfm.company,
cmfm.territory_id,
cmfm.item_code,
count(distinct pmfm.outlet_id) as pm_outlets_count,
count(distinct cmfm.outlet_id) as cm_outlets_count,
count(distinct pmfm.kyosk_delivery_note) as pm_dns_count,
count(distinct cmfm.kyosk_delivery_note) as cm_dns_count,

sum(pmfm.gmv_vat_excl) as pm_gmv_vat_excl,
sum(cmfm.gmv_vat_excl) as cm_gmv_vat_excl,

sum(pmfm.avg_cost_vat_excl) as pm_avg_cost_vat_excl,
sum(cmfm.avg_cost_vat_excl) as cm_avg_cost_vat_excl,

sum(pmfm.gmv_vat_excl) - sum(pmfm.avg_cost_vat_excl) as pm_front_margins,
sum(cmfm.gmv_vat_excl) - sum(cmfm.avg_cost_vat_excl) as cm_front_margins,
from current_month_front_margins cmfm
left join previous_month_front_margins pmfm on cmfm.territory_id = pmfm.territory_id and cmfm.item_code = pmfm.item_code
group by 1,2,3,4,5
