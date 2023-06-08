-------------------------------- Scheduled Query ---------------------------------
------------------------------- Weekly KPIs -Overall ---------------------------
-------------------------------- Created By - Rodgers ----------------------------
with
fx_rates as (
              SELECT distinct start_date, end_date, company, fx_rate_dfn, fx_rate
              FROM `kyosk-prod.uploaded_tables.uploaded_table_fx_rate_conversion_v5` 
              ),
daily_deliveries as (
                      select distinct posting_date,
                      format_date('%A',posting_date) as posting_day,
                      company,
                      customer,
                      territory,
                      count(distinct name) as count_of_dns,
                      sum(grand_total) as grand_total,
                      from `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns` 
                      --where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                      --where posting_date between '2023-05-29' and '2023-06-04'
                      group by 1,2,3,4,5
                      ),
daily_deliveries_mashup as (
                            select dn.*,
                            fxr.fx_rate_dfn,
                            fxr.fx_rate,
                            dn.grand_total / fx_rate as grand_total_in_usd
                            from daily_deliveries dn
                            left join fx_rates fxr on dn.company = fxr.company and posting_date between start_date and end_date
                            ),
weekly_delivery_notes as (
                          SELECT distinct date_trunc(posting_date, week(MONDAY)) as posting_week,
                          fx_rate_dfn,
                          count(distinct posting_date) as count_of_posting_dates,
                          count(distinct(case when posting_day not in ('Sunday') then posting_day else null end)) as count_of_sale_days,
                          count(distinct territory) as count_of_territories,
                          count(distinct customer) as count_of_customers,
                          sum(count_of_dns) as count_of_dns,
                          sum(grand_total_in_usd) as grand_total_in_usd,
                          round(sum(grand_total_in_usd) / sum(count_of_dns)) as avg_basket_value_in_usd,
                          round(sum(grand_total_in_usd) / count(distinct territory)) as gmv_per_territory_in_usd,
                          from daily_deliveries_mashup
                          group by 1,2
                          ),
weekly_dns_with_projections as (
                                select *,
                                round(grand_total_in_usd / count_of_sale_days * 6) as projected_revenue_in_usd
                                from weekly_delivery_notes
                                )
select * from weekly_dns_with_projections
order by 2,1