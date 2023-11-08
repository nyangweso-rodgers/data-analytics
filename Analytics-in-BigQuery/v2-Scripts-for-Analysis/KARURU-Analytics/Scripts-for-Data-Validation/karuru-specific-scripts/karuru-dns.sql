--------------------- Karuru ---------------
------------------ DNs Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-05'
                and is_pre_karuru = false
                ),
dns_items as (
              select distinct date(created_at) as created_at,
              country_code,
              id,
              code,
              dn.sale_order_id,
              dn.status,
              dni.product_bundle_id,
              dni.item_group_id
              from karuru_dns dn, unnest(order_items) dni
              where index = 1
              --and country_code = 'TZ'
              --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
              --and dni.status = 'ITEM_FULFILLED'
              )
select distinct product_bundle_id
from dns_items
where item_group_id is null
--order by 1 desc, code, product_bundle_id