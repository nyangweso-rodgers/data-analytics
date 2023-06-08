with
---------------------- Targets ------------------------------
date_array as (
              SELECT * 
              FROM  UNNEST(GENERATE_DATE_ARRAY(
                '2022-03-01',
                date_add(CURRENT_DATE(),interval 31 day), 
                INTERVAL 1 DAY)) AS my_date
              ),
daily_sale_days as (select * from date_array where FORMAT_DATE('%A',my_date) <> 'Sunday' ),
fx_rates as (
              SELECT distinct start_date, end_date, company, fx_rate_dfn, fx_rate
              FROM `kyosk-prod.uploaded_tables.uploaded_table_fx_rate_conversion_v5` 
              ),
targets as (
            SELECT distinct start_date,
            end_date,
            company,
            country,
            daily_gmv_in_local_currency
            FROM `kyosk-prod.uploaded_tables.upload_business_kpi_targets_v3` 
            --group by 1,2
            where country in ("TANZANIA","UGANDA","KENYA","NIGERIA")
            and start_date between '2023-05-01' and '2023-06-01'
            ),
targets_with_fx_rate as (
                        select t.*, fxr.fx_rate_dfn, fxr.fx_rate,
                        t.daily_gmv_in_local_currency / fxr.fx_rate as daily_gmv_target_in_usd
                        from targets t
                        left join fx_rates fxr on t.company = fxr.company and t.start_date between fxr.start_date and fxr.end_date
                        ),
targets_with_sales_days as (
                              select *
                              from targets_with_fx_rate, daily_sale_days
                              where my_date between start_date and end_date
                              )
-------------------->
select * 
from targets_with_sales_days
order by company, fx_rate_dfn, my_date