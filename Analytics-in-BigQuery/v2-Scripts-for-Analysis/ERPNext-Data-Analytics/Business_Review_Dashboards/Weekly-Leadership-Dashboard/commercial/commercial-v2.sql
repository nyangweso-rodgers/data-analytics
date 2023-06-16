with
dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS my_date),
daily_sale_days as (select * from dates where format_date('%A',my_date) not in ('Sunday')),
fx_rate as (
            select distinct start_date,
            end_date,
            company,
            fx_rate_dfn,
            fx_rate
            from `uploaded_tables.uploaded_table_fx_rate_conversion_v5`
            ),
targets as (
            select distinct t.start_date,
            t.end_date,
            t.company,
            country,
            daily_activated_retailers,
            cancellations,
            kyosk_app_adoption_order_count,
            kyosk_app_adoption_order_value,
            revenue_growth,
            margin,
            daily_active_outlets_monthly,
            activity_rate_monthly,
            order_frequency_monthly,
            daily_order_count,
            basket_size,
            basket_size / fxr.fx_rate as basket_size_in_usd,
            daily_revenue_per_territory,
            daily_revenue_per_territory / fxr.fx_rate as daily_gmv_per_territory_in_usd,
            daily_gmv_in_local_currency,
            daily_gmv_in_local_currency / fxr.fx_rate as daily_gmv_in_usd,
            fxr.fx_rate_dfn,
            fxr.fx_rate
            from `uploaded_tables.upload_business_kpi_targets_v3`  t, fx_rate fxr
            where t.company = fxr.company and (t.start_date between fxr.start_date and fxr.end_date) and (t.end_date between fxr.start_date and fxr.end_date)
            and  t.start_date = '2023-06-01'
            ),
daily_targets as (
                  select t.*, my_date
                  from targets t, daily_sale_days
                  where my_date between start_date and end_date
                  )
select * from daily_targets 
order by company, country,fx_rate_dfn, start_date, my_date