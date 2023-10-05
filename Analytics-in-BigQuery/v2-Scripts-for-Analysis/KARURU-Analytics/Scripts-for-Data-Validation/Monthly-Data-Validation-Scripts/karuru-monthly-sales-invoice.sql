------------------------- KARURU - Sales Invoice ------------------------------------
------------------------ Monthly Data Validations -------------------------
with

sales_invoice_with_index as (
                              SELECT *,
                              row_number()over(partition by id order by modified desc) as index
                              FROM `kyosk-prod.karuru_reports.sales_invoice`
                              WHERE date(created) >= '2022-02-01'
                              --where date(created) between '2023-01-01' and '2023-01-31'
                              --and docstatus = 1
                              --and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test TZ Territory', 'Kyosk TZ HQ')
                              and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                              --and company_id = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                              --and company_id = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                              and cast(posting_date as date) between '2023-06-01' and '2023-06-30'
                              ),
monthly_summary as (
                      select distinct
                      date_trunc(cast(posting_date as date), month) as posting_month,
                      count(distinct name)
                      from sales_invoice_with_index
                      where index =1
                      group by 1
                      )

select *
from monthly_summary