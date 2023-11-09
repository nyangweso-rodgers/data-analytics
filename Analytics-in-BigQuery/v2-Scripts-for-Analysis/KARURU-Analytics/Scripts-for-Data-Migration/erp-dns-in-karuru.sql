----------------------- ERP vs. Legacy -----------
------------DNs in Karuru ------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where date(creation) between '2022-02-01' and '2023-11-08'
            --where  territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            
            ),
erp_dns_lists as (
                select distinct date(creation) as creation,
                --customer,
                dn.name,
                dn.workflow_state,
                dn.territory,
                from erp_dns dn
                where index = 1
                order by 1 desc, name
                ),
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` 
                where date(created_at) between '2022-02-01' and '2023-11-08'
                --and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --and date(created_at) > '2023-08-05'
                and is_pre_karuru = true
                ),
legacy_dns_lists as (
                    select distinct date(created_at) as created_at,
                    id,
                    code,
                    from karuru_dns dn
                    where index = 1
                    --and country_code = 'TZ'
                    --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                    --and dni.status = 'ITEM_FULFILLED'
                    )
select *
from erp_dns_lists
where name not in (select distinct code from legacy_dns_lists)