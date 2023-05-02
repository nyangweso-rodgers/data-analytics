----------------------------- Test Script - Monthly Customer & Revenue Cohort Retention ------------------
----------------------------- Created By : Rodgers ----------------------------------------
with
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index  
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
monthly_delivery_notes as (
                            select date_trunc(posting_date, month) as posting_month,
                            customer,
                            sum(grand_total) as grand_total
                            from delivery_note_with_index
                            where index = 1
                            group by 1,2
                            ),
monthly_delivery_notes_with_index as (
                                      select *, row_number()over(partition by customer order by posting_month asc) as posting_month_index 
                                      from monthly_delivery_notes
                                      ),
cohort_joining_month as (
                            select distinct customer,
                            posting_month as joining_month,
                            grand_total
                            from monthly_delivery_notes_with_index
                            where posting_month_index = 1
                            ),
cohort_size as (
                select extract(year from joining_month) as cohort_joining_year,
                extract(month from joining_month) as cohort_joining_month, 
                count(1) as count_of_customers,
                sum(grand_total) as grand_total
                from cohort_joining_month cjm 
                group by 1,2
                order by 1,2
                )
select * from cohort_size