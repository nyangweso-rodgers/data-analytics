----------------------------- Monthly Customer & Revenue Cohort Retention ------------------
----------------------------- Created By : Rodgers ----------------------------------------
with
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index  
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where workflow_state in ('PAID', 'DELIVERED')
            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
            ),
monthly_dns as (
                select date_trunc(posting_date, month) as posting_month,
                customer,
                sum(grand_total) as grand_total,
                count(distinct name) as count_of_delivery_notes
                from erp_dns
                where index = 1
                group by 1,2
                ),
monthly_dns_with_index as (
                            select *, 
                            row_number()over(partition by customer order by posting_month asc) as posting_month_index 
                            from monthly_dns
                            ),
cohort_joining_month as (
                          select distinct customer, 
                          posting_month as joining_month,
                          grand_total,
                          count_of_delivery_notes
                          from monthly_dns_with_index  
                          where posting_month_index = 1
                          ),
-- find the size of each cohort by by counting the number of customer that show up for the first time in a month
cohort_size as (
                select extract(year from joining_month) as joining_year,
                extract(month from joining_month) as joining_month, 
                count(1) as count_of_customers,
                sum(grand_total) as grand_total,
                sum(count_of_delivery_notes) as count_of_delivery_notes
                from cohort_joining_month cjm 
                group by 1,2
                ),

customer_activities as (
                        select distinct mdn.customer,
                        date_diff(mdn.posting_month, cjm.joining_month, month) as month_number, 
                        mdn.grand_total,
                        mdn.count_of_delivery_notes
                        from monthly_dns mdn
                        left join cohort_joining_month cjm on mdn.customer = cjm.customer
                        order by 1,2
                        ),
cohort_retention_table as (
                            select extract(year from cjm.joining_month) as cohort_joining_year,
                              extract(month from cjm.joining_month) as cohort_joining_month,
                              ca.month_number,
                              sum(ca.grand_total) as grand_total,
                              sum(ca.count_of_delivery_notes) as count_of_delivery_notes,
                              count(1) as count_of_customers
                              from customer_activities ca  
                              left join cohort_joining_month cjm on ca.customer = cjm.customer
                              group by 1,2,3 order by 1,2,3
                              ),
cohort_retention_table_summary as (
                                    select distinct crt.cohort_joining_year,
                                    crt.cohort_joining_month,
                                    crt.month_number,
                                    crt.count_of_customers,
                                    crt.grand_total,
                                    crt.count_of_delivery_notes,
                                    cast(crt.count_of_customers as float64) /cs.count_of_customers as customer_cohort_percentage_retention,
                                    cast(crt.grand_total as float64) / cast(cs.grand_total as float64) as revenue_cohort_percentage_retention,
                                    cast(crt.count_of_delivery_notes as float64) / cast(cs.count_of_delivery_notes as float64) as dns_cohort_percentage_retention
                                    from cohort_retention_table crt
                                    left join cohort_size cs on crt.cohort_joining_year = cs.joining_year and crt.cohort_joining_month = cs.joining_month
                                    order by 1,2,3
                                    )
select * from cohort_retention_table_summary