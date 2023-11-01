--------------------- Karuru ---------------
------------------ DNs Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by code order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-07-01'
                and is_pre_karuru = false
                ),
karuru_dns_items as (
                    select distinct date(created_at) as created_at,
                    country_code,
                    id,
                    code,
                    dn.status,
                    dni.product_bundle_id,
                    from karuru_dns dn, unnest(order_items) dni
                    where index = 1
                    --and country_code = 'TZ'
                    --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                    --and dni.status = 'ITEM_FULFILLED'
                    )
select distinct created_at,country_code, id, code
from karuru_dns_items