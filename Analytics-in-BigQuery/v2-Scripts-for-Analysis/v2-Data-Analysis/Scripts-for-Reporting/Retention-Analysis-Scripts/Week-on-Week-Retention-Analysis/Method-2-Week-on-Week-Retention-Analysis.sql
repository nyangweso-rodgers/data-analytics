---------------------------------- Method 2: Week on Week Retention Analysis -----------------------------
---------------------------------- Created By: Rodgers -----------------------------------------
with
weekly_customer_lists as (
                            SELECT distinct date_trunc(posting_date, week) as posting_week, 
                            customer, 
                            FROM `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns` 
                            where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and territory = 'Athi River'
                            ),
customer_lists_with_index as (
                              select *, case when posting_week_index = 1 then 'ACQUIRED' else 'RETAINED' end as customer_status
                              from
                              (
                              select distinct posting_week, customer, row_number()over(partition by customer order by posting_week asc) as posting_week_index  
                              from weekly_customer_lists
                              )
                              ),
weekly_customers_count as (
                                select distinct posting_week,
                                count(distinct(case when customer_status = 'ACQUIRED' then customer else null end)) as acquired_customers,
                                count(distinct(case when customer_status = 'RETAINED' then customer else null end)) as retained_customers
                                from customer_lists_with_index
                                --where customer = 'EFRN-Royal enterprise sofia00001'
                                group by 1
                                ),
weekly_summary as (
                    select *, coalesce(lag(total_customers)over(order by posting_week asc),0) as active_customer_base
                    from
                    (select *, sum(acquired_customers)over(order by posting_week asc) total_customers
                    from
                    (select * from weekly_customers_count)
                    )
                    )
select *, coalesce(safe_divide(retained_customers , active_customer_base),0) as percent_retention   from weekly_summary
order by 1