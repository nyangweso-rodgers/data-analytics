-------------------------------- Delivery Trip, Delivery Notes , Items ------------------------------------
----------------- Sales Reconciliation Report -------------------
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
                --where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --and status not in ('CANCELLED')
                where date(created_at)  > '2021-01-01'
                --and date(created_at) >= date_sub(date_trunc(current_date() , month), interval 1 month)
                --and country_code = 'KE'
                and country_code = 'UG'
                ),
dts_cte as (
            select date(dt.created_at) as creation_date,
            country_code,
            territory_id,
            id,
            code,
            status,
            vehicle_id,
            vehicle_v2.id as vehicle_v2_id,
            vehicle_v2.license_plate as vehicle_v2_license_plate,
            case when vehicle_v2.load_capacity = '' then null else vehicle_v2.load_capacity end as vehicle_v2_load_capacity,
            case when vehicle_v2.type = '' then null else vehicle_v2.type end as vehicle_v2_type,
            vehicle_provider_id,
            vehicle_v2.service_provider_id as vehicle_v2_service_provider_id,
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
                where date(created_at) > '2021-01-01'
                --where date(created_at) >= date_sub(date_trunc(current_date() , month), interval 2 month)
                --where date(created_at) between '2024-08-19' and '2024-08-26'
              ),
dns_cte as (
            select distinct --dn.created_at,
            dn.route_id,
            dn.route_name,
            dn.fullfilment_center_id,
            dn.delivery_trip_id,
            dn.id,
            dn.code,
            --row_number()over(partition by dn.delivery_trip_id order by dn.created_at asc) as delivery_note_created_at_index,
            dn.outlet_id,
            dn.status ,
            dn.so_created_on_app,
            dn.agent_name as market_developer_name,
            dn.outlet.name as outlet_name,
            dn.outlet.phone_number as outlet_phone_number
            from delivery_notes dn
            where index =1
            ),
dns_items as (
              select distinct dn.delivery_trip_id,
              dn.id,
              dn.code,
              row_number()over(partition by dn.id order by oi.product_bundle_id asc) as delivery_note_item_index,
              oi.item_group_id,
              oi.product_bundle_id,
              oi.status as item_status,
              oi.uom,
              oi.qty_delivered * oi.discount_amount as delivery_note_discount_amount,
              oi.total_delivered - (oi.qty_delivered * oi.discount_amount) as total_delivered,
              oi.inventory_items,
              oi.total_orderd as delivery_note_ordered_amount,
              case
                when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.total_orderd - (oi.qty_delivered * oi.discount_amount)
              else 0 end as delivery_note_dispatched_amount,
              case
                when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                and oi.status in ('ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then (oi.total_orderd - oi.total_delivered) 
              else 0 end as delivery_note_removed_amount,
              case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') and oi.status in ('ITEM_CANCELLED') then oi.total_orderd else 0 end as delivery_note_cancelled_amount,
              case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.total_delivered else 0 end as gmv_vat_incl,
              case
                when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.original_item_qty
              else 0 end as delivery_note_dispatched_qty,
              case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.qty_delivered else 0 end as delivery_note_delivered_qty
              from delivery_notes dn ,unnest(order_items) oi
              where index = 1 
            ),
dns_inventory_items as (
                        select distinct dni.delivery_trip_id,
                        dni.id,
                        dni.code,
                        dni.delivery_note_item_index,
                        dni.item_group_id,
                        dni.product_bundle_id,
                        dni.item_status,
                        dni.uom,
                        --dni.original_item_qty,
                        --dni.total_ordered,
                        --dni.discount_amount,
                        --dni.qty_delivered,
                        --dni.total_delivered,
                        sum(ii.conversion_factor) as conversion_factor,
                        string_agg(distinct ii.stock_item_id ,"/" order by stock_item_id) as stock_item_id,
                        string_agg(distinct ii.uom,"/" order by ii.uom) as stock_uom,
                        sum(ii.inventory_item_qty) as inventory_item_qty,
                        dni.delivery_note_ordered_amount,
                        dni.delivery_note_dispatched_amount,
                        dni.delivery_note_discount_amount,
                        dni.delivery_note_removed_amount,
                        dni.delivery_note_cancelled_amount,
                        dni.gmv_vat_incl,
                        dni.delivery_note_dispatched_qty,
                        dni.delivery_note_delivered_qty
                        from dns_items dni,unnest(inventory_items)ii
                        group by 1,2,3,4,5,6,7,8,13,14,15,16,17,18,19,20
                      ),
-------------------- Delivery Note Settlements ----------------------------
delivery_notes_settlement_cte as (
                                    select distinct dn.id,
                                    --s.channel as settlement_channel,
                                    transaction_reference,
                                    s.amount as settlement_amount,
                                    from delivery_notes dn, unnest(settlements) s
                                    where index = 1
                                    and s.status not in ('INITIATED')
                                    ),
delivery_notes_settlement_agg as (
                                  select distinct id,
                                  --code,
                                  count(distinct transaction_reference) as transaction_reference_count,
                                  --string_agg(distinct settlement_channel) as settlement_channel,
                                  sum(settlement_amount) as settlement_amount
                                  from delivery_notes_settlement_cte
                                  group by 1
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
                when (i.weight_uom = 'Gram') then (i.weight_per_unit / 1000)
              else i.weight_per_unit end as weight_per_unit,
              --i.width,
              --i.height,
              --i.length,
              from item i--, unnest(taxes) as t
              ),
----------- Service Providers ----------
service_provider as (
                      SELECT *,
                      row_number()over(partition by id order by updated_at desc) as index
                      FROM `kyosk-prod.karuru_reports.service_provider` 
                      WHERE date(created_at) > "2021-01-01"
                      ),
service_provider_cte as (
                          select distinct 
                          id,
                          name,
                          from service_provider
                          where index = 1
                          ),
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
              --case when driver_id = '' then null else driver_id end as driver_id,
              --case when service_provider_id = '' then null else service_provider_id end as service_provider_id,
              case when type = '' then null else type end as type,
              --case when volume = '' then null else volume end as volume,
              case when load_capacity = '' then null else load_capacity end as load_capacity,
              from vehicle
              where index = 1
              ),
---------------------------- Fulfilment Centers --------------------
fulfillment_center as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.fulfillment_center` 
                        WHERE date(created_at) > "2021-06-27" #start date
                        ),
fulfillment_center_cte as (
                            select distinct 
                            id,
                            name,
                            --country_code,
                            from fulfillment_center
                            where index =1 
                            ),
------------------------- Report ----------------------------
sales_reconciliation_report as (
                                select distinct dt.creation_date as delivery_trip_creation_date,
                                dt.country_code,
                                --rm.division,
                                --rm.region,
                                rm.new_territory_id as territory_id,
                                --dn.route_id,
                                --dn.route_name,
                                --dn.fullfilment_center_id,
                                --case when fc.name = "Khetia " then 'Khetia' else rm.new_territory_id  end as fullfilment_center_name,

                                --dt.driver_code,
                                --dt.driver_name,
                                --dn.market_developer_name,
                                --dn.outlet_id,
                                --dn.outlet_name,
                                --dn.outlet_phone_number,

                                --dt.vehicle_id,
                                --dt.vehicle_v2_id,
                                --safe_cast(coalesce(v.load_capacity, dt.vehicle_v2_load_capacity, 'UNSET') as float64) as vehicle_load_capacity,
                                --coalesce(v.type, dt.vehicle_v2_type, 'UNSET') as vehicle_type,
                                --dt.vehicle_provider_id,
                                --dt.vehicle_v2_service_provider_id,
                                --dt.vehicle_v2_license_plate as vehicle_license_plate,
                                --vsp.name as vehicle_service_provider_name,
                                --v2vsp.name as vehicle_v2_service_provider_name,
                                --coalesce(v2vsp.name, vsp.name) as vehicle_service_provider_name,

                                dt.id as delivery_trip_id,
                                dt.code as delivery_trip_code,
                                dt.status as delivery_trip_status,
                                --dn.delivery_note_created_at_index,
                                --dn.created_at as delivery_note_creation_datetime,
                                dt.delivery_note_id,
                                dn.code as delivery_note_code,
                                dn.status as delivery_note_status,
                                --dn.so_created_on_app as created_on_app,

                                dnii.delivery_note_item_index,
                                dnii.product_bundle_id,
                                dnii.item_status,
                                dnii.item_group_id,
                                dnii.uom,
                                --dnii.conversion_factor,
                                dnii.stock_item_id,
                                dnii.stock_uom,
                                dnii.inventory_item_qty,
                                
                                dnii.delivery_note_ordered_amount,
                                dnii.delivery_note_dispatched_amount,
                                dnii.delivery_note_removed_amount,
                                dnii.delivery_note_cancelled_amount,
                                --dnii.delivery_note_discount_amount,
                                dnii.gmv_vat_incl,
                                case
                                  when dnii.delivery_note_item_index = 1 then dnsa.settlement_amount
                                else 0 end as delivery_note_settlement_amount,
                                --i.weight_uom,
                                --i.weight_per_unit,
                                --(i.weight_per_unit * dnii.conversion_factor * dnii.delivery_note_dispatched_qty) as delivery_note_dispatched_weight,
                                --dnii.delivery_note_dispatched_qty,
                                --dnii.delivery_note_delivered_qty,
                                from dts_cte dt
                                left join regional_mapping rm on dt.territory_id = rm.original_territory_id
                                left join dns_cte dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
                                left join dns_inventory_items dnii on dn.id = dnii.id
                                left join delivery_notes_settlement_agg dnsa on dn.id = dnsa.id
                                left join item_cte i on dnii.stock_item_id = i.item_code and dnii.stock_uom = i.stock_uom
                                left join service_provider_cte vsp on dt.vehicle_provider_id = vsp.id
                                left join service_provider_cte v2vsp on dt.vehicle_v2_service_provider_id = v2vsp.id
                                left join vehicle_cte v on dt.vehicle_id = v.id
                                left join fulfillment_center_cte fc on dn.fullfilment_center_id = fc.id
                                ),
dts_agg_cte as (
            select distinct country_code,
            territory_id,
            delivery_trip_creation_date,
            --vehicle_id,
            --vehicle_v2_id,
            --vehicle_license_plate,
            --vehicle_provider_id,
            --vehicle_v2_service_provider_id,
            --vehicle_service_provider_name,
            --vehicle_v2_service_provider_name,
            delivery_trip_id,
            delivery_trip_code, 
            delivery_trip_status, 
            count(distinct delivery_note_code) as dns_count, 
            sum(gmv_vat_incl) as gmv_vat_incl,
            --sum(delivery_note_settlement_amount) as delivery_note_settlement_amount
            from sales_reconciliation_report
            --where delivery_trip_creation_date between '2024-10-06' and '2024-10-12'
            --where vehicle_license_plate = 'KAS 106R'
            group by 1,2,3,4,5,6
            order by delivery_trip_creation_date asc
            )
/*
dns_agg_cte as (
            select distinct --delivery_trip_creation_date,
            vehicle_id,
            --vehicle_v2_id,
            vehicle_license_plate,
            vehicle_provider_id,
            --vehicle_v2_service_provider_id,
            vehicle_service_provider_name,
            --vehicle_v2_service_provider_name,
            delivery_trip_code, 
            delivery_trip_status, 
            delivery_note_code,
            count(distinct delivery_note_code) as dns_count, 
            sum(gmv_vat_incl) as gmv_vat_incl,
            sum(delivery_note_settlement_amount) as delivery_note_settlement_amount
            from sales_reconciliation_report
            where delivery_trip_creation_date between '2024-10-06' and '2024-10-12'
            --where vehicle_license_plate = 'KAS 106R'
            group by 1,2,3,4,5,6,7
            )
*/
--select * from dns_agg_cte
--select * from dts_agg_cte
--select * from delivery_notes_items where code = 'DN-KHETIA -EIKT-0HGWREJ17W2YJ'
--select * from delivery_notes_inventory_items where code = 'DN-KHETIA -EIKT-0HGWREJ17W2YJ'
--select * from delivery_trips_cte where delivery_note_id = '0HGWREJ17W2YJ'
--/*
select * from dts_agg_cte
where delivery_trip_code = 'DT-LZRA-XPO1'
--*/
/*
select * from sales_reconciliation_report 
--where delivery_trip_code = 'DT-LZRA-XPO1'
*/
--where FORMAT_DATE('%Y%m%d', delivery_trip_creation_date) between @DS_START_DATE and @DS_END_DATE --order by delivery_trip_id, delivery_note_id, product_bundle_id