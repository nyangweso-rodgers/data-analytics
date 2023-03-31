-------------------- Sales Invoice ---------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            --where posting_date between '2023-02-01' and '2023-02-28'
                            --and posting_date in ('2023-02-01', '2023-02-02', '2023-02-09')
                            where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and docstatus = 0
                            ),
sales_invoice_summary as (
                          select distinct si.posting_date, 
                          count(distinct si.name) 
                          from sales_invoice_with_index si where index = 1 
                          group by 1 order by 1
                          ),

sales_invoice_lists as (
                        select distinct si.name,
                        date(creation) as creation_date,
                        posting_date,
                        date_diff(posting_date, date(creation), day) as date_var
                        from sales_invoice_with_index si where index = 1
                        order by 2 desc
                        )

select * from sales_invoice_lists
where date_var > 0
