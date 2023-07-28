-------------- ERPNext - SO ---------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.sales_order` 
                            where date(creation) = '2023-06-17'
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                            and name in ('SAL-ORD-KANBCAU')
                            --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
                            --AND company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                            )
select distinct date(so.creation) as creation, so.name, so.workflow_state, soi.item_group, soi.item_code, soi.uom, soi.fulfilment_status, soi.amount
from sales_order_with_index so, unnest(items) soi
where index = 1