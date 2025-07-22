-----------------------------Month on Month Customer Retention By Territory --------------------------
with
deliveries as (
                select distinct customer,
                territory,
                posting_date,
                country,
                row_number()over(partition by customer order by posting_date desc) as last_posting_date_index
                from `erp_scheduled_queries.erp_paid_and_delivered_dns` 
                ),
daily_deliveries as (
                    select distinct posting_date,
                    customer,
                    from deliveries 
                    ),
customers_last_activity as (
                            select distinct customer,territory,country
                            from deliveries
                            where last_posting_date_index = 1
                            ),
monthly_mashup as (
                  select distinct dn.customer, 
                  cla.territory,
                  cla.country,
                  date_trunc(posting_date, month) as  month
                  from daily_deliveries dn
                  left join customers_last_activity cla on dn.customer = cla.customer
                  ),
monthly_retention as (
                    select 
                    last_month.country,
                    last_month.territory,
                    date_add(last_month.month, interval 1 month) as month, 
                    count(distinct last_month.customer) as active_dukas,
                    count(distinct this_month.customer) as retained_dukas, 
                    count(distinct this_month.customer) / count(distinct last_month.customer) as percent_retention
                    from monthly_mashup as last_month
                    left join monthly_mashup as this_month on last_month.customer = this_month.customer and this_month.month = date_add(last_month.month, interval 1 month)
                    group by 1,2,3
                    )
select * from monthly_retention 
where month <= date_trunc(current_date(), month)