
------------------ Delivery Notes Items, Inventory Items  ------------------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > date_sub(current_date, interval 1 month)
                --and date(created_at) > '2023-08-05'
                and country_code = 'KE'
                ),
delivery_notes_items as (
                        select distinct 
                        country_code,
                        oi.product_bundle_id, 
                        oi.uom,
                        oi.item_group_id,
                        oi.inventory_items,
                        LAST_VALUE(date(dn.created_at)) OVER (PARTITION BY dn.country_code ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_creation_datetime
                        from delivery_notes dn, unnest(order_items) oi
                        where index = 1
                        --and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                        --and oi.status = 'ITEM_FULFILLED'
                        ),
delivery_notes_inventory_items as (
                                  select distinct  dn.country_code,
                                  --dn.item_group_id,
                                  dn.product_bundle_id,
                                  dn.uom,
                                  ii.conversion_factor,
                                  ii.stock_item_id,
                                  ii.uom as stock_uom,
                                  dn.latest_creation_datetime
                                  from delivery_notes_items dn, unnest(inventory_items) ii 
                                  )
select *
from delivery_notes_inventory_items
--and stock_item_id = 'Tropical Heat Safari Puffs Chilli Lemon 12g'
--where  stock_item_id = 'Mt. Kenya Milk ESL 200ML'
--and uom is null 
order by product_bundle_id, uom