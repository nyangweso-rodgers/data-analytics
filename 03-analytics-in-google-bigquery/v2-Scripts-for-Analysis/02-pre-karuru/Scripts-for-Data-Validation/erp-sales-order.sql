-------------- ERPNext - SO ---------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.sales_order` 
                            where date(creation) >= '2023-08-01'
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                            --and name = 'SAL-ORD-MAJWNSM'
                            --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
                            --AND company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                            and territory = 'Majengo Mombasa'
                            and delivery_date = '2023-08-09'
                            )
select distinct so.name,so.creation, so.delivery_date, workflow_state, sales_partner,
soi.item_code, soi.item_group, sum(soi.amount), sum(soi.qty)
from sales_order_with_index so, unnest(items) soi
where index = 1
group by 1,2,3,4,5,6,7