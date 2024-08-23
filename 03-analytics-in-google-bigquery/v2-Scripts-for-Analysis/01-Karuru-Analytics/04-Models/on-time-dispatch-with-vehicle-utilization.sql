---------------------- Delivery Trips, Delivery Notes -------------------------
------------------------- Dispatch Summary -------------------------------
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
              case when type = '' then null else type end as type,
              case when load_capacity = '' then null else load_capacity end as load_capacity,
              from vehicle
              where index = 1
              ),
------------------------ Delivery Trips --------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and status not in ('CANCELLED', 'DISPATCHING', 'DRAFT', 'PROCESSING', 'PROCESSED', 'PUBLISHED')
                --and status in ('COMPLETED')
                and date(created_at) = '2024-08-19'
                --and date(created_at) >= date_sub(date_trunc(current_date() , month), interval 1 month)
                and country_code = 'KE'
                and id = '0H0PJB1WTX4GK'
                ),
delivery_trip_cte as (
                          select distinct date(dt.created_at) as creation_date,
                          dt.dispatched_time,
                          case 
                            when dt.country_code in ('TZ','KE','UG') then date_add(dispatched_time, interval 3 hour)
                            when dt.country_code in ('NG') then date_add(dispatched_time, interval 2 hour)
                          else dt.dispatched_time end as dispatched_time_in_local,
                          dt.id,
                          dt.code,
                          delivery_note_ids as delivery_note_id,
                          dt.country_code,
                          dt.territory_id,
                          dt.status,
                          service_provider.name as service_provider_name,
                          dt.vehicle_id,
                          vehicle_v2.license_plate as vehicle_v2_license_plate,
                          vehicle_v2.type as vehicle_v2_type,
                          case when vehicle_v2.load_capacity = '' then null else vehicle_v2.load_capacity end as vehicle_v2_load_capacity,
                          from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
                          where index = 1
                        ),
------------------------------------------ Delivery Notes --------------------------------------------
delivery_notes as (
                select *,
                row_number()over(partition by id order by updated_at desc ) as index
                from `karuru_reports.delivery_notes`
                where date(created_at) >= '2024-08-10'
                --where date(created_at) >= date_sub(date_trunc(current_date() , month), interval 2 month)
              ),
delivery_notes_items as (
                          select distinct 
                          dn.id,
                          dn.code,
                          dn.delivery_trip_id,
                          dn.status ,
                          oi.product_bundle_id,
                          oi.uom,
                          oi.status as item_status,
                          oi.inventory_items,
                          case
                            when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                            and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.total_orderd
                          else 0 end as delivery_note_dispatched_amount,
                          case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.total_delivered else 0 end as gmv_vat_incl,
                          case
                            when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                            and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.original_item_qty
                          else 0 end as delivery_note_dispatched_qty
                          --sum(oi.original_item_qty) as original_item_qty,
                          --sum(oi.total_orderd)as total_ordered,
                          --sum(oi.total_delivered - (oi.qty_delivered * oi.discount_amount)) as total_delivered,
                          from delivery_notes dn ,unnest(order_items) oi
                          where index = 1 
                          --group by 1,2,3,4,5,6,7,8
                        ),
delivery_notes_with_inventory_items as (
                                  select distinct  --dn.country_code,
                                  --dn.item_group_id,,
                                  dn.delivery_trip_id,
                                  dn.id,
                                  dn.code,
                                  dn.status,
                                  dn.product_bundle_id,
                                  dn.uom,
                                  dn.item_status,
                                  ii.conversion_factor,
                                  ii.stock_item_id,
                                  ii.uom as stock_uom,
                                  ii.dimension.metric as delivery_note_item_dimension_metric,
                                  ii.dimension.weight as delivery_note_item_dimension_weight,
                                  dn.delivery_note_dispatched_amount,
                                  dn.gmv_vat_incl,
                                  delivery_note_dispatched_qty,
                                  from delivery_notes_items dn, unnest(inventory_items) ii 
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
--------------------------------------------- Mashup ------------------
delivery_trip_with_delivery_note_items as (
                      select distinct dt.creation_date as delivery_trip_creation_date,
                      dt.dispatched_time as delivery_trip_dispatched_datetime,
                      dt.dispatched_time_in_local as delivery_trip_dispatched_datetime_in_local,
                      extract(hour from dispatched_time_in_local) as delivery_trip_hour_in_local,
                      case
                        when extract(hour from dispatched_time_in_local) <= 11 then 'On Time Dispatch'
                        when extract(hour from dispatched_time_in_local) > 11 then 'Late Dispatch'
                      else 'UNSET' end as on_time_dispatch_status,
                      dt.country_code,
                      dt.territory_id,
                      dt.id as delivery_trip_id,
                      dt.code as delivery_trip_code,
                      dt.status as delivery_trip_status,
                      dt.vehicle_id,
                      coalesce(dt.vehicle_v2_license_plate, v.license_plate) as vehicle_license_plate,
                      dt.vehicle_v2_type,
                      coalesce(v.type, dt.vehicle_v2_type) as vehicle_type,
                      dt.vehicle_v2_load_capacity,
                      coalesce(v.load_capacity, dt.vehicle_v2_load_capacity, 'UNSET') as vehicle_load_capacity,
                      dt.delivery_note_id,
                      dnwii.code as delivery_note_code,
                      dnwii.product_bundle_id,
                      dnwii.uom,
                      dnwii.conversion_factor,
                      dnwii.stock_item_id,
                      dnwii.stock_uom,
                      dnwii.delivery_note_item_dimension_metric,
                      dnwii.delivery_note_item_dimension_weight,
                      item_cte.weight_uom,
                      item_cte.weight_per_unit,
                      (item_cte.weight_per_unit * dnwii.conversion_factor * dnwii.delivery_note_dispatched_qty) as delivery_note_dispatched_weight,
                      dnwii.delivery_note_dispatched_amount,
                      dnwii.gmv_vat_incl,
                      dnwii.delivery_note_dispatched_qty,
                      from delivery_trip_cte dt
                      --left join delivery_notes_items dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
                      left join delivery_notes_with_inventory_items dnwii on dt.id = dnwii.delivery_trip_id and dt.delivery_note_id = dnwii.id
                      left join vehicle_cte v on dt.vehicle_id = v.id
                      left join item_cte on dnwii.stock_item_id = item_cte.item_code and dnwii.stock_uom = item_cte.stock_uom
                      --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
                      ),
dispatch_summary_report as (
          select distinct delivery_trip_creation_date,
          --delivery_trip_dispatched_datetime,
          --delivery_trip_dispatched_datetime_in_local,
          --delivery_trip_hour_in_local,
          country_code,
          territory_id,
          delivery_trip_id,
          delivery_trip_code,
          delivery_trip_status,
          on_time_dispatch_status,
          vehicle_id,
          vehicle_license_plate,
          vehicle_type,
          vehicle_load_capacity,
          delivery_note_id,
          delivery_note_code,
          sum(delivery_note_dispatched_weight) as delivery_note_dispatched_weight,
          sum(delivery_note_dispatched_qty) as delivery_note_dispatched_qty,
          sum(delivery_note_dispatched_amount) as delivery_note_dispatched_amount,
          sum(gmv_vat_incl) as gmv_vat_incl
          from delivery_trip_with_delivery_note_items
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
select *
--from dispatch_summary_report
from delivery_trip_with_delivery_note_items
--from delivery_notes_with_inventory_items
--where FORMAT_DATE('%Y%m%d',delivery_trip_creation_date) between @DS_START_DATE and @DS_END_DATE
--where delivery_note_code in ('DN-EMBU-0H0NWREV6X5PS')
--where delivery_note_id = '0H0N9K316X4W8'
--where product_bundle_id = "210 Wheat Flour 2KG BALE (12.0 PC)"
order by delivery_trip_creation_date desc, delivery_trip_code, delivery_note_code, product_bundle_id