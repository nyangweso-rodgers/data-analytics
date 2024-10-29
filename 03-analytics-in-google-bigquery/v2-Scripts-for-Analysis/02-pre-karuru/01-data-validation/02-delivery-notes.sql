----------------------- ERPNext - QA - DN with items --------------
with 
delivery_note as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where date(creation) between '2023-01-01' and '2023-12-31'
                            --where workflow_state in ('PAID')
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                            --and posting_date between '2023-08-01' and '2023-08-06'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and kyosk_sales_order = 'SAL-ORD-MAJWNSM'
                            and kyosk_sales_order = 'SAL-ORD-THIISHX'
                            ),
delivery_note_cte as(
                      select distinct 
                      --is_credit_sale,
                      --payment_method, # cashless and null
                      --kyosk_credit_clearance_reference_number,
                      --kyosk_order_type, # all are of types sales
                      left(name, 3) as tt
                      --kyosk_sales_order
                      from delivery_note dn 
                      where index = 1
                      )
select *
from delivery_note