----------------------- ERP ------------
----------------------DNs Items --------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where date(creation) between '2022-02-01' and '2023-11-08'
            --where territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            ),
erp_items as (
                select distinct date(dn.creation) as creation,
                company,
                --dn.territory,
                posting_date,
                --customer,
                dn.name,
                --dn.workflow_state,
                dni.item_code,
                dni.uom,
                sum(dni.base_amount) as base_amount
                from erp_dns dn, unnest(items) dni
                where index = 1
                and workflow_state in ('PAID', 'DELIVERED')
                group by 1,2,3,4,5,6
                order by 3,4,6
                ),
monthly_agg as (
                  select distinct date_trunc(creation, month) as creation_month,
                  company,
                  sum(base_amount) as base_amount
                  from erp_items
                  group by 1,2
                  order by 2, 1
                  )
select *
from erp_items