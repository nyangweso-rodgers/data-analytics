
------------------ Delivery Notes Items, Inventory Items  ------------------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > date_sub(current_date, interval 2 month)
                and status in ('PAID','DELIVERED','CASH_COLLECTED')
                --and date(created_at) > '2023-08-05'
                and country_code = 'KE'
                ),
delivery_notes_items_cte as (
                        select distinct coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                        country_code,
                        dn.territory_id,
                        oi.product_bundle_id, 
                        oi.uom,
                        oi.item_group_id,
                        oi.inventory_items,
                        sum(oi.net_total_delivered) as gmv_vat_incl,
                        sum(oi.catalog_item_qty) as catalog_item_qty,
                        sum(oi.qty_delivered) as qty_delivered 
                        --LAST_VALUE(date(dn.created_at)) OVER (PARTITION BY dn.country_code ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_creation_datetime
                        from delivery_notes dn, unnest(order_items) oi
                        where index = 1
                        and oi.status = 'ITEM_FULFILLED'
                        group by 1,2,3,4,5,6,7
                        ),
delivery_notes_inventory_items_cte as (
                                  select distinct  dn.delivery_date,
                                  dn.country_code,
                                  dn.territory_id,
                                  --dn.item_group_id,
                                  dn.product_bundle_id,
                                  dn.uom,
                                  ii.conversion_factor,
                                  ii.stock_item_id,
                                  ii.uom as stock_uom,
                                  sum(dn.catalog_item_qty) as catalog_item_qty,
                                  sum(dn.qty_delivered) as qty_delivered,
                                  sum(dn.gmv_vat_incl) as gmv_vat_incl,
                                  --dn.latest_creation_datetime
                                  from delivery_notes_items_cte dn, unnest(inventory_items) ii 
                                  group by 1,2,3,4,5,6,7,8
                                  )
select *
from delivery_notes_inventory_items_cte
where delivery_date = '2024-09-09'
--and stock_item_id = 'Tropical Heat Safari Puffs Chilli Lemon 12g'
--where  stock_item_id = 'Mt. Kenya Milk ESL 200ML'
--and uom is null 
order by territory_id, product_bundle_id, uom