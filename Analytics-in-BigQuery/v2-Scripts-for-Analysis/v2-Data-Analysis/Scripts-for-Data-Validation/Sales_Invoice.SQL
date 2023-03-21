-------------------- Sales Invoice ---------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            where posting_date between '2023-02-01' and '2023-02-28'
                            --and posting_date in ('2023-02-01', '2023-02-02', '2023-02-09')
                            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and docstatus = 0
                            ),
summary as (select distinct si.posting_date, count(distinct si.name) from sales_invoice_with_index si where index = 1 group by 1 order by 1),
summary_by_workflow_state as (select distinct si.workflow_state, count(distinct si.name) from sales_invoice_with_index si where index = 1 group by 1 order by 1),
summary_by_docstatus as (
                          select distinct docstatus, 
                          count(distinct name),
                          sum(grand_total)
                          from sales_invoice_with_index where index = 1 group by 1
                          ),

lists as (
            select  docstatus,
            si.name, si.grand_total,
            from sales_invoice_with_index si where index = 1  order by 1,2)

select * from lists
