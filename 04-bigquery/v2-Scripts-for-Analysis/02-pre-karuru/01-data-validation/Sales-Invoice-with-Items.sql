--------------------------------- Sales Invoice with Items --------------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            --where posting_date between '2023-02-01' and '2023-02-28'
                            --where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
sales_invoice_with_items as (
                              select distinct  --si.company, 
                              sii.item_code,
                              count(distinct si.customer) as count_of_customers,
                              max(posting_date) as last_invoiced_date
                              from sales_invoice_with_index si, unnest(items) sii where index = 1
                              group by 1
                              order by 1,2
                              )

select * from sales_invoice_with_items
--where item_code like '210%'