--------------------- Karuru - PAID & DELIVERED DNs ------------------------
with
delivery_note_with_index as (
                              SELECT distinct --date(created_at) as created_at_date,
                              --updated_at,
                              date(dn.delivery_date) as delivery_date,
                              --dn.paid_time,
                              --delivery_window.delivery_date as scheduled_delivery_date,

                              dn.country_code,
                              dn.territory_id,
                              --dn.agent_name,
                              dn.id,
                              dn.code,
                              dn.sale_order_id,
                              dn.delivery_trip_id,
                              dn.driver_id,
                              # driver
                              driver.name as driver_name,
                              driver.code as driver_code,
                              --driver.service_provider_id,
                              dn.status,
                              --dn.is_reschedule,
                              --dn.reschedule_from_dn_id,
                              --dn.delivery_window_id,
                              dn.outlet_id,
                              --dn.retailer_id,
                              --territory_id,
                              # Territory
                              # Order Items
                              oi.item_group_id,
                              oi.catalog_item_id,
                              oi.product_bundle_id,
                              oi.uom,
                              oi.status as order_item_status,
                              --oi.selling_price,
                              --oi.original_item_qty,
                              oi.qty_delivered,
                              --oi.total_orderd,
                              oi.total_delivered,
                              # outlet
                              --outlet.id as outlet_id,
                              --outlet.outlet_code,
                              --outlet.name,

                              is_pre_karuru,
                              row_number()over(partition by code, product_bundle_id order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.delivery_notes` dn, unnest(order_items) oi
                              where DATE(created_at) >= '2023-06-01'
                              --WHERE DATE(created_at) between '2023-08-07' and "2023-08-08"
                              --and date(delivery_date) = '2023-08-22'
                              --and is_pre_karuru = false
                              and country_code = 'KE'
                              --and territory_id not in ('DKasarani', 'Kyosk HQ', 'Test KE Territory', 'Kyosk TZ HQ', 'Test UG Territory')
                              ),
report as (
            select *
            --distinct delivery_date, code,status, sum(total_delivered)
            from delivery_note_with_index
            where index = 1
            --and status iN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
            --and order_item_status in ('ITEM_FULFILLED')
            )
select * from report
where code in ('DN-VOIM-6UL7')