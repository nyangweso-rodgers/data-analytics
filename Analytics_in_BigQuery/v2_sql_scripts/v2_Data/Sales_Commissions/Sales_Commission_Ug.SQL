---------------------------------- Sales Commission - UG -------------------------------------------------------
with
delivery_note_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.delivery_note`
                            where workflow_state in ('PAID') 
                            and company = 'KYOSK DIGITAL SERVICES LIMITED (UG)'
                            and customer = '00XW-Trust wholesalers 00001'
                            ),
daily_sales_breakdown as (
                          select *, row_number()over(partition by customer order by posting_date asc) as posting_date_index
                          from
                          (select distinct customer, sales_partner,posting_date, territory,kyosk_sales_order, created_on_app, grand_total from delivery_note_with_index where index = 1)
                          order by 1,3
                          ),
mashup as (
            select distinct sales_partner,
            string_agg(distinct territory, "/" order by territory) as territories,
            count(distinct posting_date) as count_of_posting_date,
            count(distinct customer) as count_of_customer,
            count(distinct (case when posting_date_index = 1 then customer else null end)) as count_of_new_customers,
            count(distinct (case when posting_date_index <> 1 then customer else null end)) as count_of_old_customers,
            count(distinct kyosk_sales_order) as count_of_sales_order,
            sum(grand_total) as grand_total,
            sum(case when created_on_app = 'Duka App' then grand_total else null end) as grand_total_from_duka_app,
            sum(case when created_on_app = 'Duka App' then grand_total else null end) / sum(grand_total) as proportion_of_duka_app,
            25 as duka_app_adoption_target
            from daily_sales_breakdown
            where posting_date between '2022-10-01' and '2022-10-31'
            --where FORMAT_DATE('%Y%m%d', posting_date) between @DS_START_DATE and @DS_END_DATE  
            group by 1
            ),
commission_model as (
                      select *
                      from mashup
                      )
--daily_customer_acquisitions 
select * from mashup