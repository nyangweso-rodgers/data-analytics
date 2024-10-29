----------------------------------Month on Month Activity Rate -----------------------------
------------------------------- By Territory --------------------
---------------------------------- Created By: Rodgers -----------------------------------------
with
mothly_customer_lists as (
                            SELECT distinct date_trunc(posting_date, month) as posting_month, 
                            customer, 
                            territory
                            FROM `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns` 
                            where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and territory in ('Athi River', 'Ruai')
                            --and customer in ('Ayumba shop', 'JJIB-Shangilia  shop Kikuyu Near  rubis  Petrol station 00001')
                            ),
customer_last_activity as (
                            select distinct customer, territory
                            from
                            (
                              select distinct customer,territory,row_number()over(partition by customer order by posting_month desc) as last_posting_month_index  
                              from mothly_customer_lists
                            )
                            where last_posting_month_index = 1
                            ),
customer_lists_with_index as (
                              select a.*, case when posting_month_index = 1 then 'ACQUIRED' else 'RETAINED' end as customer_status, b.territory
                              from
                              (
                              select distinct posting_month, customer, row_number()over(partition by customer order by posting_month asc) as posting_month_index  
                              from mothly_customer_lists
                              ) a left join customer_last_activity b on a.customer = b.customer
                              ),
monthly_customers_count as (
                                select distinct posting_month,
                                territory,
                                count(distinct(case when customer_status = 'ACQUIRED' then customer else null end)) as acquired_customers,
                                count(distinct(case when customer_status = 'RETAINED' then customer else null end)) as retained_customers
                                from customer_lists_with_index
                                group by 1,2
                                ),
monthly_summary as (
                    select *, coalesce(lag(total_customers)over(partition  by territory order by posting_month asc),0) as active_customer_base
                    from
                    (select *, sum(acquired_customers)over(partition  by territory order by posting_month asc) total_customers
                    from
                    (select * from monthly_customers_count)
                    )
                    ),
monthly_activity_rate_report as (
                                  select *, 
                                  coalesce(safe_divide(retained_customers , active_customer_base),0) as percent_retention   
                                  from monthly_summary
                                  )
select * from monthly_activity_rate_report
order by 2,1