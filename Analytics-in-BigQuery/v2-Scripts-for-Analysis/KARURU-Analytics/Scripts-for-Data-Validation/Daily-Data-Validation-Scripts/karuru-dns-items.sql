--------------------- Karuru ---------------
------------------ DNs Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by code order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                and date(created_at) = '2023-07-22'
                --and is_pre_karuru = true
                
                ),
karuru_dns_items as (
                    select distinct country_code,
                    territory_id,
                    date(delivery_date) as delivery_date,
                    --id,
                    code,
                    is_pre_karuru,
                    dn.status,
                    dni.product_bundle_id,
                    dni.uom,
                    dni.status as order_item_status,
                    sum(dni.total_delivered) as total_delivered
                    from karuru_dns dn, unnest(order_items) dni
                    where index = 1
                    --and date(delivery_date) between '2023-08-01' and '2022-08-31'
                    --and country_code = 'TZ'
                    --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                    --and dni.status = 'ITEM_FULFILLED'
                    group by 1,2,3,4,5,6,7,8,9
                    order by 3,6,product_bundle_id, uom
                    )
select *
from karuru_dns_items
where code = "DN-MWEN-IMMT"