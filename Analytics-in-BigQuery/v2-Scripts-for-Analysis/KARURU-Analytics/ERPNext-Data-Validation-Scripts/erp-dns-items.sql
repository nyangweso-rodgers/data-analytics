----------------------- ERPNext ------------
----------------------DNs Items --------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            and date(creation) = '2023-07-22'
            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
            --'KYOSK DIGITAL SERVICES LIMITED (UG)'
            --'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
            ),
erp_items as (
                select distinct company,
                dn.territory,
                posting_date,
                --customer,
                dn.name,
                dn.workflow_state,
                dni.item_code,
                dni.uom,
                sum(dni.base_amount) as base_amount
                from erp_dns dn, unnest(items) dni
                where index = 1
                --and workflow_state in ('PAID', 'DELIVERED')
                --and posting_date between '2023-08-01' and '2023-08-31'
                and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                group by 1,2,3,4,5,6,7
                order by 3,4,6
                )
select *
from erp_items
where name = "DN-MWEN-IMMT"