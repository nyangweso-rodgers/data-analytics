with
weekly_dns_kpis as (
                    SELECT distinct date_trunc(posting_date, WEEK(MONDAY)) as posting_week,
                    fx_rate_dfn,
                    count(distinct posting_date) as count_of_posting_dates,
                    count(distinct(case when format_date('%A',posting_date) not in ('Sunday') then posting_date else null end)) as count_of_sale_days,
                    count(distinct territory) as count_of_territories,
                    count(distinct customer) as count_of_customers,
                    count(distinct delivery_notes) as count_of_dns,
                    sum(grand_total_in_usd) as grand_total_in_usd,
                    round(sum(grand_total_in_usd) / count(distinct delivery_notes)) as avg_basket_value_in_usd,
                    round(sum(grand_total_in_usd) / count(distinct territory)) as gmv_per_territory_in_usd,
                    FROM `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns_v2` 
                    --where posting_date between '2023-05-29' and '2023-06-11'
                    group by 1,2
                    ),
weekly_dns_with_projections as (
                                select *,
                                round(grand_total_in_usd / count_of_sale_days * 6) as projected_revenue_in_usd
                                from weekly_dns_kpis
                                ),
mashup as (
            select *, 
            coalesce(lag(projected_revenue_in_usd)over(partition by fx_rate_dfn order by posting_week asc ),0) as previous_wk_gmv_projection
            from weekly_dns_with_projections
            )
select *, safe_divide(projected_revenue_in_usd , previous_wk_gmv_projection)-1 as proj_gmv_growth
from mashup
order by 2,1