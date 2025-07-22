----------------------------------Month on Month Activity Rate -----------------------------
-------------------------- Overall ------------------------
---------------------------------- Created By: Rodgers -----------------------------------------
with
mothly_customer_lists as (
                            SELECT distinct date_trunc(posting_date, month) as posting_month, 
                            customer, 
                            FROM `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns` 
                            where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and territory in ('Juja', 'Eldoret')
                            ),
customer_lists_with_index as (
                              select *, case when posting_month_index = 1 then 'ACQUIRED' else 'RETAINED' end as customer_status
                              from
                              (
                              select distinct posting_month, customer, row_number()over(partition by customer order by posting_month asc) as posting_month_index  
                              from mothly_customer_lists
                              )
                              ),
monthly_customers_count as (
                                select distinct posting_month,
                                count(distinct(case when customer_status = 'ACQUIRED' then customer else null end)) as acquired_customers,
                                count(distinct(case when customer_status = 'RETAINED' then customer else null end)) as retained_customers
                                from customer_lists_with_index
                                --where customer = 'EFRN-Royal enterprise sofia00001'
                                group by 1
                                ),
activity_rate_report as (
                        select *, coalesce(lag(total_customers)over(order by posting_month asc),0) as active_customer_base
                        from
                        (select *, sum(acquired_customers)over(order by posting_month asc) total_customers
                        from
                        (select * from monthly_customers_count)
                        )
                        )
select *, coalesce(safe_divide(retained_customers , active_customer_base),0) as percent_retention   from activity_rate_report
order by 1 desc