-------------- ERPNext - SO ---------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.sales_order` 
                            where date(creation) between '2023-07-01' and '2023-07-31'
                            --and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                            --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
                            and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                            ),
sales_order_summary as (
                        select distinct  so.company, so.name, so.workflow_state,
                        from sales_order_with_index so
                        where index = 1
                        )
select * from sales_order_summary
order by 1,2