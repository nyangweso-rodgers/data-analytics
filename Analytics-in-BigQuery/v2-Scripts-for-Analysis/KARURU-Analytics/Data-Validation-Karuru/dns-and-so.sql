---------------------- KARURU - Mashup  --------------------
with
sales_order_with_index as (
                            SELECT distinct date(created_date) as created_date,
                            #delivery window
                            --delivery_window.id as delivery_window_id,
                            --delivery_window.delivery_date,
                            --delivery_window.start_time as delivery_window_start_time,
                            --delivery_window.end_time as delivery_window_end_time,
                            id,
                            name,
                            # outlets
                            --outlet.name as outlet_name,
                            --outlet.outlet_code,
                            so.outlet_id,
                            --retailer_id,
                            order_status,
                            --legacy_id,
                            # territory
                            territory.country_code,
                            territory.id as territory_id,
                            # Market Developer
                            market_developer.id as market_developer_id,
                            market_developer.first_name,
                            market_developer.last_name,
                            --market_developer.phone_number,
                            # Items
                            i.fulfilment_status,
                            i.catalog_item_id,
                            category_id,
                            product_bundle_id,
                            i.uom,
                            i.catalog_item_qty,
                            i.discount_amount,
                            i.selling_price,
                            i.total,
                            --paid_total,
                            --so.total_amount,
                            --i.net_total,
                            is_pre_karuru,
                            row_number()over(partition by name,product_bundle_id  order by last_modified_date desc) as index
                            FROM `kyosk-prod.karuru_reports.sales_order` so, unnest(items) i
                            --WHERE date(created_date) between '2023-07-01' and '2023-07-31'
                            WHERE date(created_date) >= '2023-08-01'
                            and is_pre_karuru = false
                            and territory_id not in ('DKasarani', 'Kyosk HQ', 'Test KE Territory', 'Kyosk TZ HQ', 'Test UG Territory')
                            --and id = 'SO-0D7AM32WH0E8S'
                            ),
delivery_note_with_index as (
                              SELECT distinct date(created_at) as created_at_date,
                              --updated_at,
                              dn.delivery_date,
                              --dn.paid_time,
                              --delivery_window.delivery_date as scheduled_delivery_date,

                              --dn.country_code,
                              dn.territory_id,
                              dn.agent_name,
                              # MD
                              market_developer.id as market_developer_id,
                              market_developer.first_name,
                              market_developer.last_name,
                              dn.id,
                              dn.code,
                              dn.sale_order_id,
                              dn.delivery_trip_id,
                              dn.driver_id,
                              
                              # driver
                              driver.name as driver_name,
                              driver.code as driver_code,
                              driver.service_provider_id,
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
                              oi.selling_price,
                              oi.original_item_qty,
                              oi.qty_delivered,
                              oi.total_orderd,
                              oi.total_delivered,
                              # outlet
                              --outlet.id as outlet_id,
                              --outlet.outlet_code,
                              --outlet.name,

                              is_pre_karuru,
                              row_number()over(partition by code, product_bundle_id order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.delivery_notes` dn, unnest(order_items) oi
                              --WHERE DATE(created_at) between '2022-08-01' and "2023-07-29"
                              WHERE DATE(created_at) >= '2023-07-10'
                              and is_pre_karuru = false
                              --and country_code = 'UG'
                              and territory_id not in ('DKasarani', 'Kyosk HQ', 'Test KE Territory', 'Kyosk TZ HQ', 'Test UG Territory')
                              and code in ('DN-KWMP-0RT9', 'DN-KWMP-8ARH')
                              ),
sales_order_delivery_note_mashup as (
                                      select distinct dnwi.id,
                                      dnwi.code,
                                      dnwi.sale_order_id,
                                      dnwi.status,
                                      --sowi.id,
                                      sowi.name,
                                      sowi.created_date,
                                      sowi.order_status,
                                      dnwi.item_group_id,
                                      dnwi.catalog_item_id,
                                      dnwi.product_bundle_id,
                                      dnwi.uom,
                                      --dnwi.order_item_status,
                                      --sowi.catalog_item_id
                                      --sowi.fulfilment_status,
                                      --dnwi.total_orderd,
                                      --dnwi.total_delivered,
                                      sowi.total_amount,
                                      sowi.total,
                                      --dnwi.original_item_qty,
                                      --dnwi.qty_delivered,
                                      --sowi.catalog_item_qty,
                                      --sowi.selling_price,
                                      --dnwi.selling_price
                                      --dnwi.agent_name,
                                      --dnwi.market_developer_id,
                                      --sowi.market_developer_id,
                                      --dnwi.first_name,
                                      --sowi.first_name
                                      from delivery_note_with_index dnwi
                                      left join sales_order_with_index sowi on dnwi.sale_order_id = sowi.id and dnwi.catalog_item_id = sowi.catalog_item_id and dnwi.uom = sowi.uom
                                      where dnwi.index = 1 
                                      and sowi.index = 1
                                      )
select * from sales_order_delivery_note_mashup
order by 1