
-------------- DTs and DNs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and status not in ('CANCELLED')
                --where date(created_at) = current_date
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 week)
                and  date(created_at) between '2024-06-01' and '2024-06-30'
                --and is_pre_karuru = false
                and country_code = 'KE'
                and territory_id = 'Kisumu1'
              ),
delivery_trips_cte as (
                      select distinct date(created_at) as created_at,
                      --created_at,
                      --updated_at,
                      --bq_upload_time,
                      country_code,
                      territory_id,
                      id,
                      code,
                      status,
                      --vehicle.id as vehicle_id,
                      --vehicle.licence_plate,
                      --vehicle.vehicle_type,
                      delivery_note_ids as delivery_note_id,
                      --driver.id as driver_id,
                      --driver.code as driver_code,
                      --driver.name as driver_name,
                      vehicle_id,
                      --service_provider.id as service_provider_id,
                      --service_provider.name as service_provider_name
                      from delivery_trips, unnest(delivery_note_ids) delivery_note_ids
                      where index = 1
                    ),
------------------------------- Delivery Notes -------------------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 week)
                --where date(created_at) > date_sub(current_date, interval 2 month)
                where  date(created_at) between '2024-05-01' and '2024-06-30'
                --and status in ('PAID','DELIVERED','CASH_COLLECTED')
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
              select distinct date(created_at) as created_at,
              --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
              dn.country_code,
              dn.territory_id,
              --route_id,
              --route_name,
              delivery_trip_id,
              id,
              code,
              --dn.is_reschedule,
              --dn.reschedule_from_dn_id,
              --dn.sale_order_id,
              --dn.sale_order_code,
              dn.status,
              --delivery_trip_id,
              --payment_request_id,
              --agent_name as market_developer,
              --outlet.phone_number,
              outlet_id,
              --outlet.name as outlet_name,
              oi.status as dn_item_status,
              oi.item_group_id,
              oi.product_bundle_id,
              oi.uom,
              --oi.qty_delivered,
              --oi.total_orderd,
              --oi.total_delivered - (oi.discount_amount * qty_delivered) as total_delivered,
              case
                when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.total_orderd
              else 0 end as total_dispatched,
              case
                when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.total_delivered
              else 0 end as total_delivered,
              from delivery_notes dn, unnest(order_items) oi
              where index = 1
              --and oi.status in ('ITEM_FULFILLED')
              ),
----------------------------------------- Front Margins ------------------
front_margins_report as (
                        SELECT 
                          distinct --territory_id,
                          kyosk_delivery_note,
                          delivery_note,
                          item_code,
                          uom,
                          sum(base_net_amount) as base_net_amount,
                          sum(total_incoming_rate) as total_incoming_rate
                        FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                        wHERE delivery_date > date_sub(date_trunc(current_date, month), interval 2 month)
                        group by 1,2,3,4
                        ),
--------------------------- Item Group Mapping -----------------------------
item_group_mapping as (
                        SELECT distinct country_code,
                        item_group_id,
                        type
                        FROM `kyosk-prod.karuru_upload_tables.item_group_mapping` 
                        where country_code = 'KE'
                        ),
--------------------------------------------- Vehicle ----------------------------------------------
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
--------------------------------------------- Vehicle Type -----------------------------
vehicle_type as (  
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index 
                  FROM `kyosk-prod.karuru_reports.vehicle_type` 
                  WHERE date(created_at) >='2023-10-13'
                  ),
vehicle_type_cte as (
                select distinct --date(created_at) as created_at,
                id,
                code,
                car_type,
                vehicle_capacity 
                from vehicle_type
                where index = 1
                ),
----------------------- Mashups ------------------------
deliveries_and_invoices_mashup as (
                  select dt.created_at as delivery_trip_creation_date,
                  dt.country_code,
                  dt.territory_id,
                  --dn.route_id,
                  --dn.route_name,
                  dn.outlet_id,
                  --dn.outlet_name,
                  dt.id as delivery_trip_id,
                  dt.code as delivery_trip_code,
                  dt.status as delivery_trip_status,
                  v.license_plate,
                  vt.car_type,
                  dt.delivery_note_id,
                  dn.code as delivery_note_code,
                  --dn.sale_order_id,
                  --dn.is_reschedule,
                  --dn.reschedule_from_dn_id,
                  --dt.vehicle_id,
                  --v.license_plate,
                  dn.status as delivery_note_status,
                  dt.vehicle_id,
                  igm.type as item_group_type,
                  dn.item_group_id,
                  dn.product_bundle_id,
                  dn.uom,
                  dn.dn_item_status as delivery_note_item_status,
                  dn.total_dispatched as dispatched_amount_vat_incl,
                  dn.total_delivered as gmv_vat_incl,
                  fmr.base_net_amount as gmv_vat_excl,
                  fmr.total_incoming_rate as avg_cost_vat_excl,
                  fmr.base_net_amount - fmr.total_incoming_rate as front_margin_vat_excl,
                  from delivery_trips_cte dt
                  left join delivery_notes_cte dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
                  left join front_margins_report fmr on dn.code = fmr.delivery_note and dn.product_bundle_id = fmr.item_code and dn.uom = fmr.uom
                  left join item_group_mapping igm on dn.item_group_id = igm.item_group_id and dn.country_code = igm.country_code
                  left join vehicle_cte v on dt.vehicle_id = v.id
                  left join vehicle_type_cte vt on v.vehicle_type_id = vt.id
                  ),
summary_by_delivery_trip as (
                              select distinct delivery_trip_creation_date,
                              country_code,
                              territory_id,
                              delivery_trip_id,
                              delivery_trip_code,
                              delivery_trip_status,
                              license_plate,
                              car_type,
                              count(distinct outlet_id) as count_of_outles,
                              count(distinct delivery_note_id) as count_of_delivery_notes,
                              count(distinct item_group_id) as counf_of_item_groups,
                              count(distinct product_bundle_id) as count_of_skus,
                              --sum(case when delivery_note_status in ('PAID','DELIVERED','CASH_COLLECTED') and  delivery_note_item_status in ('ITEM_FULFILLED') then gmv_vat_incl else 0 end) as gmv_vat_incl,
                              --sum(case when delivery_note_status in ('PAID','DELIVERED','CASH_COLLECTED') and  delivery_note_item_status in ('ITEM_FULFILLED') then gmv_vat_excl else 0 end) as gmv_vat_excl,4
                              sum(dispatched_amount_vat_incl) as dispatched_amount_vat_incl,
                              sum(gmv_vat_incl) as gmv_vat_incl,
                              round(sum(gmv_vat_incl) / sum(dispatched_amount_vat_incl),2) as order_fulfilment_rate,
                              sum(gmv_vat_excl) as gmv_vat_excl,
                              round(sum(front_margin_vat_excl),2) as front_margin_vat_excl,
                              round(sum(front_margin_vat_excl) / sum(gmv_vat_excl),2) as front_margin_percent,
                              sum(case when item_group_type in ('Revenue Movers') then gmv_vat_excl else 0 end) as revenue_movers_gmv_vat_excl,
                              sum(case when item_group_type in ('Margin Movers') then gmv_vat_excl else 0 end) as margin_movers_gmv_vat_excl,
                              sum(case when item_group_type in ('Assortment') then gmv_vat_excl else 0 end) as assortment_gmv_vat_excl,
                              sum(case when item_group_type in ('New Category') then gmv_vat_excl else 0 end) as new_category_gmv_vat_excl,

                              round(sum(case when item_group_type in ('Revenue Movers') then gmv_vat_excl else 0 end) / sum(gmv_vat_excl),2) as revenue_movers_gmv_contribution,
                              round(sum(case when item_group_type in ('Margin Movers') then gmv_vat_excl else 0 end) / sum(gmv_vat_excl),2) as margin_movers_gmv_contribution,
                              round(sum(case when item_group_type in ('Assortment') then gmv_vat_excl else 0 end) / sum(gmv_vat_excl),2) as assortment_gmv_contribution,
                              round(sum(case when item_group_type in ('New Category') then gmv_vat_excl else 0 end) / sum(gmv_vat_excl),0) as new_category_gmv_contribution,
                               --AND dn.status 
                              --and dni.status = 'ITEM_FULFILLED'
                              from deliveries_and_invoices_mashup
                              group by 1,2,3,4,5,6,7,8
                              )
select *
--from delivery_trips_cte
--from deliveries_and_invoices_mashup
from summary_by_delivery_trip

--where territory_id in ('Ruiru')

--where dn_status IN ('PAID','DELIVERED','CASH_COLLECTED')
--and dn_item_status = 'ITEM_FULFILLED'
