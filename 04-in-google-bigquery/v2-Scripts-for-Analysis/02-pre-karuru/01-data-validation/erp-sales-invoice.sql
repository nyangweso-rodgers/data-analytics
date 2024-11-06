--------------------ERPNext - QA - Sales Invoice ---------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            where docstatus = 1
                            --and date(creation) >= '2022-02-01'
                            and date(creation) between '2023-07-01' and '2023-07-31'
                            --and territory not in ('Test UG Territory', 'Test NG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test TZ Territory', 'Kyosk TZ HQ')
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                            ),
sales_invoice_summary as (
                          select distinct date(creation), 
                          si.name,
                          si.grand_total
                          from sales_invoice_with_index si where index = 1 
                          order by 1
                          ),

sales_invoice_lists as (
                        select distinct si.name, si.status,
                        grand_total,
                        date(creation),
                        territory
                        from sales_invoice_with_index si where index = 1
                        )

select *
from sales_invoice_lists
where name in ("SI-MNIL-ET4R-23",
"ACC-SINV-2023-72059")