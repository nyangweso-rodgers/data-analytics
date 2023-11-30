----------------------- ERP ------------
----------------------DNs Items --------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            and date(creation) between '2022-02-01' and '2023-11-29'
            ),
dn_items as (
                select distinct --date(dn.creation) as creation,
                company,
                --dn.name,
                dni.item_code,
                dni.uom,
                --sum(dni.base_amount) as base_amount
                from erp_dns dn, unnest(items) dni
                where index = 1
                --and workflow_state in ('PAID', 'DELIVERED')
                )
select *
from erp_items