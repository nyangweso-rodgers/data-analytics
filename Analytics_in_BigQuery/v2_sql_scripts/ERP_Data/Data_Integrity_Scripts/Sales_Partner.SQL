--------------- Sales Partner -----------------
with
sales_partner_with_index  as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index  
                              FROM `kyosk-prod.erp_reports.sales_partner`
                              ),
sales_partner_summary as (
                          select *
                          from sales_partner_with_index where index = 1 order by 1
                          )
select distinct name from sales_partner_summary