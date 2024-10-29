--------------------ERPNext - QA - Sales Invoice ---------------
with
sales_invoice as (
                  SELECT *, 
                  row_number()over(partition by name order by modified desc) as index 
                  FROM `kyosk-prod.erp_reports.sales_invoice` 
                  where docstatus = 1
                  --and date(creation) >= '2022-02-01'
                  and date(creation) between '2023-01-01' and '2023-12-31'
                  and territory not in ('Test UG Territory', 'Test NG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test TZ Territory', 'Kyosk TZ HQ')
                  and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                  --and company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                  ),
sales_invoice_cte as (
                      select distinct --creation, 
                      si.name,
                      si.debit_to,
                      left(si.name, 3) as tt
                      --si.grand_total
                      from sales_invoice si where index = 1 
                      order by 1
                      )

select *
from sales_invoice
where po_no = 'SAL-ORD-THIISHX'