--------------------ERPNext - QA - Sales Invoice ---------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            where docstatus = 1
                            --and date(creation) >= '2022-02-01'
                            and date(creation) between '2023-08-01' and '2023-08-27'
                            ),
sales_invoice_summary as (
                          select distinct date(creation) as creation, 
                          si.name,
                          si.grand_total,
                          status
                          from sales_invoice_with_index si where index = 1 
                          and territory not in ('Test UG Territory', 'Test NG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test TZ Territory', 'Kyosk TZ HQ')
                          --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                          --and company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                          )
select *
from sales_invoice_summary