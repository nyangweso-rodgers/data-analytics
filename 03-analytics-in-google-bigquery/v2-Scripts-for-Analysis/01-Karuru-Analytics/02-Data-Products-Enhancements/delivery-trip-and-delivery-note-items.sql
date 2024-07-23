---------------------- Delivery Trips, Delivery Notes -------------------------
------------------------- Driver Sales Recnciliations Report -------------------------------
with
-------------------------------------- Vehicle ----------------
vehicle as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle` 
            WHERE date(created_at) >= '2023-10-01'
            ),
vehicle_cte as (
              select distinct
              id,
              license_plate,
              code,
              vehicle_type_id
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
------------------------ Delivery Trips --------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where date(created_at) = '2024-07-20'
               --where date(created_at) >= date_sub(date_trunc(current_date() , month), interval 1 month)
                ),
delivery_trip_cte as (
                          select distinct date(dt.created_at) as creation_date,
                          dt.id,
                          dt.code,
                          delivery_note_ids as delivery_note_id,
                          dt.country_code,
                          dt.territory_id,
                          --rm.territory_renamed as territory_id, 
                          --rm.division,
                          --rm.region,
                          dt.status,
                          dt.estimated_value,
                          dt.delivered_value,
                          dt.driver.code as driver_code,
                          dt.driver.name as driver_name,
                          dt.driver.phone_number as driver_phone_number,
                          service_provider.name as service_provider_name,
                          dt.vehicle_id,
                          --dt.driver_provider_id,
                          --dt.vehicle_provider_id,
                          vehicle.licence_plate,
                          vehicle.vehicle_type,
                          service_provider.id as service_provider_id,
                          
                          --dnr.inventory_item_qty,
                          --dnr.original_item_qty,
                          --dnr.total_ordered,
                          --dnr.discount_amount,
                          --dnr.qty_delivered,
                          --dnr.total_delivered,
                          --dnr.original_item_qty - dnr.qty_delivered as qty_variance,
                          --dnr.total_ordered - dnr.total_delivered as amount_variance
                          from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
                          --left join regional_mapping rm on dt.territory_id = rm.territory
                          --left join dns_report dnr on dt.id = dnr.delivery_trip_id
                          where index = 1
                        ),
--------------------------------------------------------------------------------------
delivery_notes as (
                select *,
                row_number()over(partition by id order by updated_at desc ) as index
                from `karuru_reports.delivery_notes`
                where date(created_at) >= '2024-07-19'
                --where date(created_at) >= date_sub(date_trunc(current_date() , month), interval 1 month)
                --and is_pre_karuru = false
              ),
delivery_notes_items as (
                          select date(created_at) as creation_date,
                          dn.id,
                          dn.code,
                          dn.delivery_trip_id,
                          dn.outlet_id,
                          dn.status ,
                          dn.so_created_on_app,
                          dn.agent_name as market_developer_name,
                          dn.outlet.name as outlet_name,
                          dn.outlet.phone_number as outlet_phone_number,
                          oi.product_bundle_id,
                          oi.status as item_status,
                          oi.uom,
                          oi.original_item_qty,
                          oi.total_orderd - (oi.catalog_item_qty * oi.discount_amount) as total_ordered,
                          oi.qty_delivered,
                          oi.qty_delivered * oi.discount_amount as discount_amount,
                          oi.total_delivered - (oi.qty_delivered * oi.discount_amount) as total_delivered,
                          --oi.inventory_items
                          from delivery_notes dn ,unnest(order_items) oi
                          where index = 1 
                        ),
--------------------------------------------- Mashup ------------------
mashup as (
          select distinct dt.creation_date as dt_creation_date,
          dt.country_code,
          dt.territory_id,
          dt.vehicle_id,
          v.license_plate,
          --dt.licence_plate,
          dt.vehicle_type,
          dt.id as delivery_trip_id,
          dt.code as delivery_trip_code,
          dt.status as delivery_trip_status,
          dt.estimated_value as dt_estimated_value,
          dt.delivered_value as dt_delivered_value,
          dt.delivery_note_id,
          dn.code as delivery_note_code,
          dn.creation_date as dn_creation_date,
          dn.status as delivery_note_status,
          dn.product_bundle_id,
          dn.uom,
          dn.item_status,
          dn.total_ordered,
          case
            when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') and dn.item_status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then dn.total_ordered
          else 0 end as total_dispatched,
          case
            when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') and dn.item_status in ('ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then (dn.total_ordered - dn.total_delivered) else 0 end as total_removed,
          case
            when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') and dn.item_status in ('ITEM_CANCELLED') then dn.total_ordered
          else 0 end as total_cancelled,
          dn.total_delivered,
          dn.discount_amount,
          dn.original_item_qty,
          case
            when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') and dn.item_status in ('ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then (dn.original_item_qty - dn.qty_delivered) else 0 end as qty_removed,
          dn.qty_delivered,
          dn.outlet_id,
          dn.outlet_name,
          dn.outlet_phone_number,
          dt.driver_code,
          dt.driver_name,
          dt.driver_phone_number,
          dn.market_developer_name,
          dn.so_created_on_app as created_on_app,
          dt.service_provider_id,
          dt.service_provider_name,
          
          from delivery_trip_cte dt
          left join delivery_notes_items dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
          left join vehicle_cte v on dt.vehicle_id = v.id
          )
select *,
from mashup
where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
and delivery_trip_status not in ('CANCELLED')
and delivery_trip_id = '0GQ0WWHV1PJQR'
--and delivery_note_code in ('DN-KWMP-0GPRGEKQNRB4X')
order by delivery_trip_id, delivery_note_id, product_bundle_id