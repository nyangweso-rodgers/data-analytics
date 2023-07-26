-------------- ERPNext - SO ---------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.sales_order` 
                            where date(creation) >= '2023-04-01'
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                            --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
                            --AND company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                            )
select distinct workflow_state 
from sales_order_with_index where index = 1