--------------------ERPNext - QA - Sales Invoice ---------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            --where date(creation) >= '2023-04-01'
                            where posting_date between '2023-08-01' and '2023-08-06'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
sales_invoice_summary as (
                          select distinct si.posting_date, 
                          count(distinct si.name) 
                          from sales_invoice_with_index si where index = 1 
                          group by 1 order by 1
                          ),

sales_invoice_lists as (
                        select distinct si.name,
                        grand_total
                        from sales_invoice_with_index si where index = 1
                        )

select * from sales_invoice_lists 