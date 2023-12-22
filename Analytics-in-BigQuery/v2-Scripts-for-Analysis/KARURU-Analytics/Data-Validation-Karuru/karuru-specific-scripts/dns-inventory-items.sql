--------------------- Karuru ---------------
------------------ DNs Inventory Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-05'
                and is_pre_karuru = false
                ),
dn_items as (
              select 
              country_code,
              oi.product_bundle_id, 
              oi.inventory_items 
              from karuru_dns dn, unnest(order_items) oi
              where index = 1
              ),
dn_inventory_items as (
                      select distinct  dn.country_code,
                      dn.product_bundle_id,
                      ii.stock_item_id,
                      ii.uom
                      from dn_items dn, unnest(inventory_items) ii 
                      )
select *
from dn_inventory_items
where uom is null 