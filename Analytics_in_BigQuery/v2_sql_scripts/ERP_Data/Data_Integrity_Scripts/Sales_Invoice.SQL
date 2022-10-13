-------------------- Sales Invoice ---------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            --where docstatus = 1
                            --where posting_date between '2022-10-01' and '2022-10-03'
                            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                            ),
sales_invoice_summary as (select distinct posting_date, count(distinct name) as count_of_sales_invoices from sales_invoice_with_index where index = 1 group by 1 order by 1,2),
sales_invoice_lists as (select distinct posting_date,name, grand_total from sales_invoice_with_index where index = 1 order by 1,2)

select count(*) from sales_invoice_lists