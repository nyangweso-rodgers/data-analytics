-------------- ERPNext - SO ---------------------
with
sales_order as (
                SELECT *,
                row_number()over(partition by name order by modified desc) as index
                FROM `kyosk-prod.erp_reports.sales_order` 
                where date(creation) between '2023-07-01' and '2023-07-31'
                --and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                --and name = 'SAL-ORD-MAJWNSM'
                --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
                --AND company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                --and territory = 'Majengo Mombasa'
                --and delivery_date = '2023-08-09'
                and name = 'SAL-ORD-THIISHX'

                ),
sales_order_cte as (
                    select distinct so.creation, 
                    so.transaction_date,
                    so.delivery_date,

                    so.company,
                    so.territory,

                    so.customer,
                    so.customer_name,

                    so.order_type,
                    so.name,

                    so.base_total,
                    so.base_net_total,
                    so.base_grand_total,
                    so.grand_total,
                    from sales_order so
                    where index = 1
                    )
select *
from sales_order_cte