-------------------------------- Delivery Trip, Delivery Notes , Items ------------------------------------
----------------- Driver Sales Reconciliation Report -------------------
-----------------------------------Created By : Rodgers -----------------------------------------------
with
-------------------------------Uploaded Tables---------------------------------------
regional_mapping as (
                    select distinct country,
                    region,
                    sub_region,
                    division,
                    original_territory_id, 
                    new_territory_id,
                    from `kyosk-prod.karuru_upload_tables.territory_region_mapping` 
                    ),

------------------------------------------ Delivery Trips ----------------------------------------------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and status not in ('CANCELLED')
                --and date(created_at) >= date_sub(date_trunc(current_date() , month), interval 1 month)
                and date(created_at) between '2024-08-12' and '2024-08-19'
                --and code in ('DT-EMBU-R9CV', 'DT-EAST-CD3L')
                --and country_code = 'KE'
                ),
delivery_trips_cte as (
                        select date(dt.created_at) as creation_date,
                        country_code,
                        territory_id,
                        id,
                        code,
                        status,
                        driver.code as driver_code,
                        driver.name as driver_name,
                        delivery_note_ids as delivery_note_id
                        from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
                        ),
--------------------------- Delivery Notes ---------------------------------
delivery_notes as (
                select *,
                row_number()over(partition by id order by updated_at desc ) as index
                from `karuru_reports.delivery_notes`
                --where date(created_at) >= date_sub(date_trunc(current_date() , month), interval 1 month)
                where date(created_at) between '2024-08-12' and '2024-08-19'
              ),
delivery_notes_items as (
              select distinct 
              dn.route_id,
              dn.route_name,
              dn.id,
              dn.code,
              dn.delivery_trip_id,
              dn.outlet_id,
              dn.status ,
              dn.created_on_app,
              dn.agent_name as market_developer_name,
              dn.outlet.name as outlet_name,
              dn.outlet.phone_number as outlet_phone_number,
              oi.product_bundle_id,
              oi.status as item_status,
              oi.uom,
              --oi.original_item_qty,
              --oi.total_orderd - (oi.catalog_item_qty * oi.discount_amount) as total_ordered,
              --oi.qty_delivered,
              oi.qty_delivered * oi.discount_amount as delivery_note_discount_amount,
              oi.total_delivered - (oi.qty_delivered * oi.discount_amount) as total_delivered,
              oi.inventory_items,
              oi.total_orderd as delivery_note_ordered_amount,
              case
                when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                /*and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED')*/ then oi.net_total_ordered
              else 0 end as delivery_note_dispatched_amount,
              case
                when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                and oi.status in ('ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then (oi.total_orderd - oi.total_delivered) 
              else 0 end as delivery_note_removed_amount,
              case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') and oi.status in ('ITEM_CANCELLED') then oi.total_orderd else 0 end as delivery_note_cancelled_amount,
              case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.net_total_delivered else 0 end as gmv_vat_incl,
              case
                when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.original_item_qty
              else 0 end as delivery_note_dispatched_qty,
              case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.qty_delivered else 0 end as delivery_note_delivered_qty
              from delivery_notes dn ,unnest(order_items) oi
              where index = 1 
            ),
delivery_notes_with_inventory_items as (
                                      select distinct dni.id,
                                      dni.route_id,
                                      dni.route_name,
                                      dni.code,
                                      dni.delivery_trip_id,
                                      dni.outlet_id,
                                      dni.status,
                                      dni.created_on_app,
                                      dni.market_developer_name,
                                      dni.outlet_name,
                                      dni.outlet_phone_number,
                                      dni.product_bundle_id,
                                      dni.item_status,
                                      dni.uom,
                                      --dni.original_item_qty,
                                      --dni.total_ordered,
                                      --dni.discount_amount,
                                      --dni.qty_delivered,
                                      --dni.total_delivered,
                                      ii.conversion_factor,
                                      ii.stock_item_id,
                                      ii.uom as stock_uom,
                                      ii.inventory_item_qty,
                                      dni.delivery_note_ordered_amount,
                                      dni.delivery_note_dispatched_amount,
                                      dni.delivery_note_discount_amount,
                                      dni.delivery_note_removed_amount,
                                      dni.delivery_note_cancelled_amount,
                                      dni.gmv_vat_incl,
                                      dni.delivery_note_dispatched_qty,
                                      dni.delivery_note_delivered_qty
                                      from delivery_notes_items dni,unnest(inventory_items)ii
                                    ),
----------------------------- item -------------
item as (
        SELECT * 
        FROM `kyosk-prod.karuru_reports.item` 
        WHERE date(creation) > '2022-02-01'
        --and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
        and maintain_stock = true
        ),
item_cte as (
              select distinct 
              i.company_id,
              i.id,
              i.item_code,
              i.item_name,
              --i.item_group_id,
              --i.maintain_stock,
              --i.disabled,
              i.stock_uom,
              case
                when i.weight_uom = 'Gram' then 'Kg' 
              else i.weight_uom end as weight_uom,
              case
                when i.weight_uom = 'Gram' then i.weight_per_unit / 1000
              else i.weight_per_unit end as weight_per_unit,
              --i.width,
              --i.height,
              --i.length,
              from item i, unnest(taxes) as t
              ),
------------------------- Report ----------------------------
driver_reconciliation_report as (
                          select distinct dt.creation_date as delivery_trip_creation_date,
                          dt.country_code,
                          rm.division,
                          rm.region,
                          rm.new_territory_id as territory_id,
                          dnwii.route_id,
                          dnwii.route_name,
                          dt.driver_code,
                          dt.driver_name,
                          dt.id as delivery_trip_id,
                          dt.code as delivery_trip_code,
                          dt.status as delivery_trip_status,
                          --service_provider.name as service_provider_name,
                          --dnr.creation_date as dn_creation_date,
                          dnwii.created_on_app,
                          dt.delivery_note_id,
                          dnwii.code as delivery_note_code,
                          dnwii.status as delivery_note_status,
                          dnwii.item_status,
                          dnwii.product_bundle_id,
                          dnwii.uom,
                          dnwii.conversion_factor,
                          dnwii.stock_item_id,
                          dnwii.stock_uom,
                          item_cte.weight_uom,
                          item_cte.weight_per_unit,
                          (item_cte.weight_per_unit * dnwii.conversion_factor * dnwii.delivery_note_dispatched_qty) as delivery_note_dispatched_weight,
                          dnwii.market_developer_name,
                          dnwii.outlet_id,
                          dnwii.outlet_name,
                          dnwii.outlet_phone_number,
                          dnwii.inventory_item_qty,
                          dnwii.delivery_note_ordered_amount,
                          dnwii.delivery_note_dispatched_amount,
                          dnwii.gmv_vat_incl,
                          --dnwii.delivery_note_dispatched_amount - dnwii.gmv_vat_incl as delivery_note_returned_amount,
                          dnwii.delivery_note_removed_amount,
                          dnwii.delivery_note_cancelled_amount,
                          dnwii.delivery_note_discount_amount,
                          dnwii.delivery_note_dispatched_qty,
                          dnwii.delivery_note_delivered_qty
                          from delivery_trips_cte dt
                          left join regional_mapping rm on dt.territory_id = rm.original_territory_id
                          left join delivery_notes_with_inventory_items dnwii on dt.id = dnwii.delivery_trip_id and dt.delivery_note_id = dnwii.id
                          left join item_cte on dnwii.stock_item_id = item_cte.item_code and dnwii.stock_uom = item_cte.stock_uom
                        )
select 
distinct delivery_note_status, item_status
--distinct delivery_trip_creation_date, delivery_trip_id, count(distinct delivery_note_id) as delivery_note_id
from driver_reconciliation_report
--where FORMAT_DATE('%Y%m%d', delivery_trip_creation_date) between @DS_START_DATE and @DS_END_DATE
order by 1,2