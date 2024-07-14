
------------------ DNs Inventory Items  ------------------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where date(created_at) > date_sub(current_date, interval 3 month)
                --and date(created_at) > '2023-08-05'
                --and is_pre_karuru = false
                ),
delivery_notes_items as (
                        select dn.delivery_window.delivery_date as scheduled_delivery_date,
                        date(dn.delivery_date) as delivery_date,
                        country_code,
                        dn.territory_id,
                        oi.product_bundle_id, 
                        oi.item_group_id,
                        oi.inventory_items 
                        from delivery_notes dn, unnest(order_items) oi
                        where index = 1
                        --and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                        --and oi.status = 'ITEM_FULFILLED'
                        ),
delivery_notes_inventory_items as (
                                  select distinct  
                                  dn.country_code,
                                  dn.territory_id,
                                  scheduled_delivery_date,
                                  --dn.product_bundle_id,
                                  dn.item_group_id,
                                  ii.stock_item_id,
                                  ii.uom as stock_uom,
                                  sum(ii.inventory_item_qty) as inventory_item_qty
                                  from delivery_notes_items dn, unnest(inventory_items) ii 
                                  group by 1,2,3,4,5,6
                                  ),
delivery_notes_inventory_items_with_index as (
                                              select *,
                                              row_number()over(partition by territory_id, stock_item_id,stock_uom order by scheduled_delivery_date desc) as scheduled_delivery_date_index,
                                              --sum(inventory_item_qty) over(partition by territory_id, stock_item_id,stock_uom order by scheduled_delivery_date desc) as inventory_item_total_qty
                                              from delivery_notes_inventory_items
                                              order by territory_id, stock_item_id, scheduled_delivery_date
                                              ),
inventory_items_daily_demand as (
                                  select distinct country_code,
                                  territory_id,
                                  stock_item_id,
                                  stock_uom,
                                  count(distinct scheduled_delivery_date) as count_scheduled_delivery_date,
                                  sum(inventory_item_qty) as inventory_item_qty,
                                  round(sum(inventory_item_qty) / count(distinct scheduled_delivery_date),0) as inventory_item_qty_daily_demand
                                  from delivery_notes_inventory_items_with_index
                                  where scheduled_delivery_date_index <= 4
                                  group by 1,2,3,4
                                  )
select distinct stock_item_id, item_group_id
from delivery_notes_inventory_items
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
--and country_code = 'KE'
--and scheduled_delivery_date_index <= 4
--and stock_item_id = 'Tropical Heat Safari Puffs Chilli Lemon 12g'
and  stock_item_id = 'Mt. Kenya Milk ESL 200ML'
--and uom is null 