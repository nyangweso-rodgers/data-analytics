----------------------- mashup - sales order, delivery notes --------------------------
with
-------- uploaded tables ----------------
uploaded_zero_md_routes_cte as (
                                SELECT distinct country_id,
                                territory_id,
                                route_id,
                                route_name,
                                zero_md_route
                                FROM `kyosk-prod.karuru_upload_tables.zero_md_routes` 
                                where zero_md_route = true
                                ),
uploaded_regional_mapping_cte as (
                    select distinct country,
                    --region,
                    --sub_region,
                    --division,
                    original_territory_id,
                    new_territory_id,
                    from `kyosk-prod.karuru_upload_tables.territory_region_mapping`
                    ),
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                --and order_status not in ('INITIATED')
                --and territory.country_code = 'ke'
                --where territory_id = 'Ruiru'
                --and date(created_date) between '2024-09-01' and '2024-09-12' 
                where date(created_date) >= date_sub(current_date, interval 1 month)
                --and id = 'SO-0HKMX1VCDRVSG' # test reschedueles
                ),
so_items_cte as (
                select distinct so.created_date,

                --so.last_modified_date,
                --so.bq_upload_time,

                so.territory.country_code as country_code,
                so.territory_id,
                so.route.id as route_id,
                so.route.route_name as route_name,

                so.outlet_id,
                so.outlet.name as outle_name,
                 so.outlet.phone_number as outlet_phone_number,
                so.created_on_app,
                so.id,
                so.name,
                --so.market_developer.id as market_developer_id,
                --so.market_developer_name,
                so.order_status,

                i.category_id,
                i.product_bundle_id,
                i.uom,
                i.fulfilment_status,
                --i.catalog_item_qty,
                --i.selling_price,
                --i.total,
                i.net_total,
                --i.discount_amount,
                i.inventory_items,
                --i.promotion_type,
                --i.promotion_on,
                --i.discount_type
                from sales_order so, unnest(items) i
                where index = 1
                ),
so_inventory_items_cte as (
                          select distinct soi.created_date,
                          --soi.last_modified_date,
                          --soi.bq_upload_time,

                          soi.country_code,
                          soi.territory_id,
                          ii.fulfilment_center_id,
                          ii.fulfilment_center_name,
                          soi.route_id,
                          soi.route_name,

                          soi.outlet_id,
                          soi.outle_name,
                          soi.outlet_phone_number,

                          soi.created_on_app,
                          soi.id,
                          soi.name,
                          soi.order_status,

                          --soi.market_developer_id,
                          --soi.market_developer_name,

                          
                          soi.category_id,
                          soi.product_bundle_id,
                          soi.uom,
                          soi.fulfilment_status,
                          --soi.catalog_item_qty,
                          --soi.selling_price,
                          sum(soi.net_total) as net_total,
                          --soi.discount_amount,
                          --string_agg(distinct cast(ii.conversion_factor as string), "/" order by cast(ii.conversion_factor as string)) as conversion_factor,
                          string_agg(distinct ii.stock_item_id, "/" order by ii.stock_item_id) as stock_item_id,
                          string_agg(distinct ii.stock_uom, "/" order by ii.stock_uom) as stock_uom,
                          --sum(ii.inventory_item_qty) as inventory_item_qty,
                          --soi.promotion_type,
                          --soi.promotion_on,
                          --soi.discount_type
                          from so_items_cte soi, unnest(inventory_items) ii
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
                          ),
/*
so_agg_cte as (
                select distinct date(created_date) as date,
                country_code,
                territory_id,
                fulfilment_center_id,
                fulfilment_center_name,
                count(distinct id) as sales_order_count,
                sum(net_total) as sales_order_amount
                from so_inventory_items_cte, vars
                where date(created_date) between vars.current_start_date and vars.current_end_date
                and fulfilment_center_name = 'Khetia ' 
                group by 1,2,3,4,5
                ),
*/
------------------------------ Delivery Notes ------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                where date(created_at) > date_sub(current_date, interval 1 month)
                --and fullfilment_center_id = '0HEHY3146QXKF'
                --and code = 'DN-KHETIA -EIKT-0HNE8EYHB4127'
                --and id = '0J2B7T60KCP1K'
                and is_reschedule = false
                ),
/*
dns_status_change_history_cte as (
                        select distinct dn.id,
                        sch.user_id as user_id,
                        sch.from_status as from_status,
                        sch.to_status as to_status,
                        sch.change_time as change_time
                        --dn.status_change_history,
                        from delivery_notes dn, unnest(status_change_history) sch
                        where index = 1
                        ),
*/
dn_items_cte as (
                  select distinct --dn.created_at,
                  dn.scheduled_delivery_date,
                  --d.change_time as dispatched_datetime,
                  --oc.change_time as ops_cancelled_datetime,
                  --uc.change_time as user_cancelled_datetime,
                  dn.delivery_window_id,
                  coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                   case 
                    when dn.country_code in ('TZ','KE','UG') then date_add(delivery_date, interval 3 hour)
                    when dn.country_code in ('NG') then date_add(delivery_date, interval 2 hour)
                  else dn.delivery_date end as delivery_date_in_local,

                  country_code,
                  territory_id,
                  --dn.fullfilment_center_id as fulfilment_center_id,
                  --route_id,
                  --route_name,

                  dn.id,
                  dn.code,
                  dn.sale_order_id,
                  dn.sale_order_code,

                  dn.is_reschedule,
                  dn.status,
                  
                  delivery_trip_id,
                  --payment_request_id,
                  --agent_name as market_developer,

                  --outlet_id,
                  --outlet.name as outlet_name,
                  --outlet.outlet_code as outlet_code,

                  --oi.item_group_id,
                  oi.product_bundle_id,
                  oi.uom,
                  oi.status as item_status,
                  oi.inventory_items,

                  --sum(oi.total_orderd) as total_orderd,
                  --sum(oi.total_delivered) as  
                  --oi.total_delivered,
                  --oi.net_total_delivered,
                  oi.net_total_ordered,
                  case when dn.status in ('USER_CANCELLED') then oi.net_total_ordered else 0 end as dn_user_cancelled_amount,
                  case when dn.status in ('OPS_CANCELLED') then oi.net_total_ordered else 0 end as dn_ops_cancelled_amount,
                  case
                    when dn.status in ('DISPATCHED', 'CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED', 'RESCHEDULED') 
                    and oi.status in ('ITEM_CANCELLED', 'ITEM_FULFILLED', 'ITEM_DISPATCHED', 'ITEM_RESCHEDULED') then oi.total_orderd - (oi.qty_delivered * oi.discount_amount)
                  else 0 end as dn_dispatched_amount,
                  case
                    when dn.status in ('RESCHEDULED')
                    and oi.status in ('ITEM_RESCHEDULED') then oi.total_orderd - (oi.qty_delivered * oi.discount_amount)
                  else 0 end as dn_rescheduled_amount,
                  case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED') and oi.status in ('ITEM_CANCELLED') then oi.total_orderd else 0 end as dn_cancelled_amount,
                  case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.net_total_delivered else 0 end as gmv_vat_incl,
                  from delivery_notes dn, unnest(order_items) oi
                  --left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'DISPATCHED') d on dn.id = d.id
                  --left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'OPS_CANCELLED') oc on dn.id = oc.id
                  --left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'USER_CANCELLED') uc on dn.id = uc.id
                  where index = 1
                  --and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                  --and oi.status = 'ITEM_FULFILLED'
                  ),
dn_inventory_items_cte as (
                          select distinct  dni.scheduled_delivery_date,
                          dni.delivery_date,
                          dni.delivery_date_in_local,
                          dni.delivery_window_id,
                          
                          dni.country_code,
                          --dni.territory_id,
                          --dni.outlet_id,
                          dni.id,
                          dni.code,
                          dni.sale_order_id,
                          --dni.sale_order_code,
                          dni.is_reschedule,
                          dni.status,
                          dni.delivery_trip_id,

                          --dni.item_group_id,
                          dni.product_bundle_id,
                          dni.uom,
                          dni.item_status,
                          --sum(dni.catalog_item_qty) as catalog_item_qty,
                          --sum(dni.qty_delivered) as qty_delivered,
                          --sum(dni.gmv_vat_incl) as gmv_vat_incl,
                          --dni.catalog_item_qty,
                          --dni.qty_delivered,
                          --dni.net_total_delivered,
                          dni.net_total_ordered,
                          dni.dn_user_cancelled_amount,
                          dni.dn_ops_cancelled_amount,
                          dni.dn_dispatched_amount,
                          dni.dn_rescheduled_amount,
                          dni.dn_cancelled_amount,
                          dni.gmv_vat_incl,

                          sum(ii.conversion_factor) as conversion_factor,
                          string_agg(distinct ii.stock_item_id ,"/" order by stock_item_id) as stock_item_id,
                          string_agg(distinct ii.uom,"/" order by ii.uom) as stock_uom,
                          sum(ii.inventory_item_qty) as inventory_item_qty,

                          --string_agg(distinct ii.dimension.metric, "/" order by ii.dimension.metric) as dimension_metric,
                          --string_agg(distinct cast(ii.dimension.length as string), "/" order by cast(ii.dimension.length as string)) as dimension_length
                          from dn_items_cte dni, unnest(inventory_items) ii 
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
                          ),
/*
dn_dispatch_agg_cte as (
                        select distinct date(dispatched_datetime) as date,
                        territory_id,
                        fulfilment_center_id,
                        sum(delivery_note_dispatched_amount) as delivery_note_dispatched_amount
                        from dn_items_cte, vars
                        where date(dispatched_datetime) between vars.current_start_date and vars.current_end_date 
                        group by 1,2,3
                        ),
dn_user_cancelled_agg_cte as (
                        select distinct date(user_cancelled_datetime) as date,
                        territory_id,
                        fulfilment_center_id,
                        sum(dn_user_cancelled_amount) as dn_user_cancelled_amount
                        from dn_items_cte, vars
                        where date(user_cancelled_datetime) between vars.current_start_date and vars.current_end_date 
                        group by 1,2,3
                        ),
dn_ops_cancelled_agg_cte as (
                        select distinct date(ops_cancelled_datetime) as date,
                        territory_id,
                        fulfilment_center_id,
                        sum(dn_ops_cancelled_amount) as dn_ops_cancelled_amount
                        from dn_items_cte, vars
                        where date(ops_cancelled_datetime) between vars.current_start_date and vars.current_end_date 
                        group by 1,2,3
                        ),
dn_delivered_agg_cte as (
                      select distinct date(delivery_date) as date,
                      territory_id,
                      fulfilment_center_id,
                      count(distinct case when status in ('PAID','DELIVERED','CASH_COLLECTED') then id else null end) as delivery_note_count,
                      sum(delivery_note_cancelled_amount) + sum(dn_rescheduled_amount) as delivery_note_returned_amount,
                      sum(gmv_vat_incl) as gmv_vat_incl
                      from dn_items_cte, vars
                      where date(delivery_date) between vars.current_start_date and vars.current_end_date 
                      group by 1,2,3
                      ),
*/
------------------------------ Delivery Trips -----------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 month)
                --where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --where date(created_at) = current_date
                --and status not in ('CANCELLED')
                --and fulfillment_center_id = '0HEHY3146QXKF'
              ),
dt_cte as (
            select distinct --created_at,
            case 
                when dt.country_code in ('KE','UG','TZ') then TIMESTAMP_ADD(dt.created_at, interval 3 HOUR) 
                when dt.country_code = 'NG' then TIMESTAMP_ADD(dt.created_at, interval 1 HOUR) 
            else dt.created_at end as creation_time_in_local,

            --country_code,
            --territory_id,
            --fulfillment_center_id as fulfilment_center_id,
            id,
            code,
            status,
            --dispatched_value,

            delivery_note_ids as delivery_note_id
            from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
            where index = 1
          ),
dt_status_change_history_cte as (    
                                select distinct dt.id,
                                sch.from_status,
                                sch.to_status,
                                sch.change_time,
                                case
                                  when dt.country_code in ('KE','UG','TZ') then TIMESTAMP_ADD(sch.change_time, interval 3 HOUR)
                                  when dt.country_code = 'NG' then TIMESTAMP_ADD(sch.change_time, interval 1 HOUR) 
                                else sch.change_time end as change_time_in_local
                                --case when sch.to_status = 'COMPLETED' then sch.change_time end as completed_time
                                --max(case when sch.to_status = 'COMPLETED' then sch.change_time end)  as completed_time,
                                --max(case when sch.to_status = 'DISPATCHED' then sch.change_time end)  as dispatched_time,
                                --max(case when sch.to_status = 'DELIVERED' then sch.change_time end)  as delivered_time
                                from delivery_trips dt,unnest(status_change_history) sch
                                --where index = 1 
                                --group by 1
                                ),
/*
dt_agg_cte as (
                select distinct date(created_at) as date,
                territory_id,
                fulfilment_center_id,
                count(distinct id) as delivery_trip_count,
                sum(dispatched_value) as delivery_trip_dispatched_value
                from dt_cte, vars
                where date(created_at) between vars.current_start_date and vars.current_end_date
                group by 1,2,3
                ),
*/
----------------------- delivery window v3 -----------------------------
delivery_window_v3 as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index
                        FROM `kyosk-prod.karuru_reports.delivery_window_v3` 
                        WHERE TIMESTAMP_TRUNC(created_at, DAY) > TIMESTAMP("2024-01-01")
                        ),
delivery_window_v3_cte as (
                            select distinct created_at,
                            --updated_at,
                            --bq_upload_time,
                            id,
                            --delivery_window_config_id,
                            available,
                            --route_cluster_id,
                            cut_off_time,
                            start_time,
                            end_time
                            from delivery_window_v3
                            where index = 1
                            ),
--------------------------- Mashup ----------------------
so_and_dn_inventory_items_cte as (
                            select distinct date(soii.created_date) as so_creation_date,
                            
                            soii.country_code,
                            --soii.territory_id,
                            urm.new_territory_id as territory_id,
                            soii.fulfilment_center_id,
                            
                            --soii.fulfilment_center_name,
                            case when soii.fulfilment_center_name = "Khetia " then 'Khetia' else urm.new_territory_id  end as fulfilment_center_name,
                            soii.route_id,
                            soii.route_name,
                            uploaded_zero_md_routes_cte.zero_md_route,

                            soii.outlet_id,
                            soii.outle_name,
                            soii.outlet_phone_number,

                            soii.created_on_app,
                            soii.id as so_id,
                            soii.name as so_code,
                            soii.order_status as so_status,

                            soii.category_id,
                            soii.product_bundle_id,
                            soii.uom,
                            soii.fulfilment_status as so_item_fulfilment_status,
                            soii.net_total as so_amount,
                            soii.stock_item_id,
                            soii.stock_uom,

                            --date(dni.created_at) as dn_creation_date,
                            --date(dni.dispatched_datetime) as dn_dispatched_date,
                            dnii.scheduled_delivery_date as dn_scheduled_delivery_date,
                            dnii.delivery_window_id,
                            extract(hour from (case
                              when dnii.country_code in ('TZ','KE','UG') then date_add(dwv3.start_time, interval 3 hour)
                              when dnii.country_code in  ('NG') then date_add(dwv3.start_time, interval 2 hour)
                            else dwv3.start_time end)) as delivery_window_v3_local_start_time,
                            extract(hour from (case
                              when dnii.country_code in ('TZ','KE','UG') then date_add(dwv3.end_time, interval 3 hour)
                              when dnii.country_code in  ('NG') then date_add(dwv3.end_time, interval 2 hour)
                            else dwv3.end_time  end)) as delivery_window_v3_local_end_time,
                            date(dnii.delivery_date) as dn_delivery_date,
                            EXTRACT(HOUR FROM dnii.delivery_date_in_local) as delivery_hour,
                            dnii.id as dn_id,
                            dnii.code as dn_code,
                            dnii.is_reschedule as dn_is_reschedule,
                            dnii.status as dn_status,
                            dnii.item_status as dn_item_status,
                            coalesce(dnii.net_total_ordered, 0) as dn_net_total_ordered,
                            coalesce(dnii.dn_user_cancelled_amount, 0) as dn_user_cancelled_amount,
                            coalesce(dnii.dn_ops_cancelled_amount, 0) as dn_ops_cancelled_amount,
                            coalesce(dnii.dn_dispatched_amount, 0) as dn_dispatched_amount,
                            coalesce(dnii.dn_rescheduled_amount, 0) as dn_rescheduled_amount,
                            coalesce(dnii.dn_cancelled_amount, 0) as dn_cancelled_amount,
                            coalesce(dnii.dn_rescheduled_amount + dnii.dn_cancelled_amount, 0) as dn_returned_amount,
                            coalesce(dnii.gmv_vat_incl, 0) as gmv_vat_incl,
                            case
                              when (dt.code is not null) then (dnii.net_total_ordered - dnii.dn_dispatched_amount)
                            else 0 end as vmi_out_of_stock_amount,

                            dnii.delivery_trip_id,
                            dt.code as dt_code,
                            dt.creation_time_in_local as dt_local_creation_datetime,
                            dtschdt.change_time_in_local as dt_dispatched_datetime,
                            date_diff(dtschdt.change_time_in_local, dt.creation_time_in_local, minute) as dt_creation_to_dispatch_in_min,
                            --date(dt.created_at) as dt_creation_date,
                            --dni.delivery_trip_id as dt_id,
                            dt.status as dt_status,

                            --date(soii.created_date) = date(dni.created_at) as check_so_and_dn_creation_date,
                            from so_inventory_items_cte soii
                            --left join dn_items_cte dni on soii.id = dni.sale_order_id and soii.product_bundle_id = dni.product_bundle_id and soii.uom = dni.uom
                            --full outer join dn_items_cte dni on soii.id = dni.sale_order_id and soii.product_bundle_id = dni.product_bundle_id and soii.uom = dni.uom
                            full outer join dn_inventory_items_cte dnii on soii.id = dnii.sale_order_id  and soii.stock_item_id = dnii.stock_item_id and soii.stock_uom = dnii.stock_uom 
                            left join dt_cte dt on dnii.delivery_trip_id = dt.id and dnii.id = dt.delivery_note_id
                            left join uploaded_zero_md_routes_cte on soii.route_id = uploaded_zero_md_routes_cte.route_id
                            left join uploaded_regional_mapping_cte urm on soii.territory_id = urm.original_territory_id
                            left join delivery_window_v3_cte dwv3 on dnii.delivery_window_id = dwv3.id
                            left join (select distinct id, change_time_in_local from dt_status_change_history_cte where to_status = 'DISPATCHED') dtschdt on dt.id = dtschdt.id
                            ),
calculate_otif_report_cte as (
                select *,
                case 
                  when (dn_delivery_date = dn_scheduled_delivery_date) and (delivery_hour between delivery_window_v3_local_start_time and delivery_window_v3_local_end_time) then 'ON-TIME DELIVERY' 
                  when (dn_delivery_date > dn_scheduled_delivery_date) or (dn_delivery_date = dn_scheduled_delivery_date and delivery_hour > delivery_window_v3_local_end_time) then 'LATE DELIVERY'
                  when (dn_delivery_date < dn_scheduled_delivery_date) or (dn_delivery_date = dn_scheduled_delivery_date and delivery_hour < delivery_window_v3_local_start_time)  then 'EARLY DELIVERY'
                else 'UNSET' end as otif_status,
                from so_and_dn_inventory_items_cte
                )
                            /*,
so_dn_and_dt_cte as (
                    select distinct so_creation_date,
                    
                    country_code,
                    territory_id,
                    fulfilment_center_id,
                    fulfilment_center_name,

                    so_id,
                    so_status,

                    dn_creation_date,
                    dn_dispatched_date,
                    dn_delivery_date,
                    dn_id,
                    dn_status,
                    sum(dn_dispatched_amount) as dn_dispatched_amount,
                    sum(dn_rescheduled_amount) as dn_rescheduled_amount,
                    sum(dn_cancelled_amount) as dn_cancelled_amount,
                    sum(dn_returned_amount) as dn_returned_amount,
                    sum(gmv_vat_incl) as gmv_vat_incl,
                    sum(dn_user_cancelled_amount) as dn_user_cancelled_amount,
                    sum(dn_ops_cancelled_amount) as dn_ops_cancelled_amount
                    from so_dn_and_dt_items_cte
                    group by 1,2,3,4,5,6,7,8,9,10,11,12
                    order by so_creation_date desc, so_id
                    ),*/
/*
so_dn_and_dt_agg_cte as (
                        select so_agg_cte.*, 

                        coalesce(dn_dispatch_agg_cte.delivery_note_dispatched_amount,0) as delivery_note_dispatched_amount,

                        coalesce(dn_delivered_agg_cte.delivery_note_count, 0) as delivery_note_count,
                        coalesce(dn_delivered_agg_cte.delivery_note_returned_amount, 0) as delivery_note_returned_amount,
                        coalesce(dn_delivered_agg_cte.gmv_vat_incl, 0) as gmv_vat_incl,

                        coalesce(dt_agg_cte.delivery_trip_count, 0) as delivery_trip_count,

                        coalesce(dn_user_cancelled_agg_cte.dn_user_cancelled_amount, 0) as dn_user_cancelled_amount,
                        coalesce(dn_ops_cancelled_agg_cte.dn_ops_cancelled_amount, 0) as dn_ops_cancelled_amount,
                        --coalesce(dt_agg_cte.delivery_trip_dispatched_value, 0) as delivery_trip_dispatched_value

                        coalesce(front_margins_agg_cte.base_net_amount, 0) as base_net_amount,
                        coalesce(front_margins_agg_cte.total_incoming_rate, 0) as total_incoming_rate
                        from so_agg_cte
                        full outer join dn_dispatch_agg_cte using(date, fulfilment_center_id)
                        full outer join dn_delivered_agg_cte using(date, fulfilment_center_id) --on soa.date = dna.date and soa.fulfilment_center_id = dna.fulfilment_center_id
                        full outer join dt_agg_cte using(date, fulfilment_center_id) 
                        full outer join dn_user_cancelled_agg_cte using(date, fulfilment_center_id) 
                        full outer join dn_ops_cancelled_agg_cte using(date, fulfilment_center_id) 
                        full outer join front_margins_agg_cte using(date, fulfilment_center_id) 
                        order by date desc
                        )*/
--select * from so_dn_and_dt_items_cte
select * from calculate_otif_report_cte
--where country_code = 'ke' and so_creation_date = '2024-12-02'
--where fulfilment_center_id = '0HEHY3146QXKF'
--where fulfilment_center_name = 'Khetia '
--where so_code = 'SOSCIM22024' 
where FORMAT_DATE('%Y%m%d', so_creation_date) between @DS_START_DATE and @DS_END_DATE