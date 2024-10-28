with    
------------------------ Date Variabes ------------------------------
vars AS (
  --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE)  as current_end_date ),
  SELECT DATE '2024-10-14' as current_start_date,  DATE '2024-10-24' as current_end_date ),
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                --and order_status not in ('INITIATED')
                --and territory.country_code = 'ke'
                where territory_id = 'Ruiru'
                --and date(created_date) between '2024-09-01' and '2024-09-12' 
                and date(created_date) >= date_sub(current_date, interval 1 month)
                --and id = 'SO-0HKMX1VCDRVSG' # test reschedueles
                --and id = 'SO-0HNE8EXM9A0Z2'
                ),
so_items_cte as (
                select distinct so.created_date,

                --so.last_modified_date,
                --so.bq_upload_time,

                so.territory.country_code as country_code,
                so.territory_id,
                --so.route.id as route_id,
                --so.route.route_name as route_name,

                --so.outlet_id,
                so.id,
                so.name,
                --so.created_on_app,
                --so.market_developer.id as market_developer_id,
                --so.market_developer_name,
                so.order_status,

                --i.category_id,
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
                          --soi.route_id,
                          --soi.route_name,

                          --soi.outlet_id,
                          soi.id,
                          soi.name,
                          --soi.created_on_app,
                          soi.order_status,

                          --soi.market_developer_id,
                          --soi.market_developer_name,

                          
                          --soi.category_id,
                          soi.product_bundle_id,
                          soi.uom,
                          soi.fulfilment_status,
                          --soi.catalog_item_qty,
                          --soi.selling_price,
                          sum(soi.net_total) as net_total,
                          --soi.discount_amount,
                          --string_agg(distinct cast(ii.conversion_factor as string), "/" order by cast(ii.conversion_factor as string)) as conversion_factor,
                          --string_agg(distinct ii.stock_item_id, "/" order by ii.stock_item_id) as stock_item_id,
                          --string_agg(distinct ii.stock_uom, "/" order by ii.stock_uom) as stock_uom,
                          --sum(ii.inventory_item_qty) as inventory_item_qty,
                          --soi.promotion_type,
                          --soi.promotion_on,
                          --soi.discount_type
                          from so_items_cte soi, unnest(inventory_items) ii
                          group by 1,2,3,4,5,6,7,8,9,10,11 
                          ),
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
------------------------------ Delivery Notes ------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > date_sub(current_date, interval 1 month)
                and fullfilment_center_id = '0HEHY3146QXKF'
                --and code = 'DN-KHETIA -EIKT-0HNE8EYHB4127'
                ),
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
dn_settlement_cte as (
                      select distinct dn.id,
                      --s.channel as settlement_channel,
                      --s.transaction_reference,
                      sum(s.amount) as settlement_amount,
                      from delivery_notes dn, unnest(settlements) s
                      where index = 1
                      --and s.status not in ('INITIATED')
                      group by 1
                      ),
dn_items_cte as (
                  select distinct dn.created_at,
                  d.change_time as dispatched_datetime,
                  oc.change_time as ops_cancelled_datetime,
                  uc.change_time as user_cancelled_datetime,
                  coalesce(date(delivery_date), date(updated_at)) as delivery_date,

                  --country_code,
                  territory_id,
                  dn.fullfilment_center_id as fulfilment_center_id,
                  --route_id,
                  --route_name,

                  dn.id,
                  dn.code,
                  dn.sale_order_id,
                  --dn.sale_order_code,

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
                  else 0 end as delivery_note_dispatched_amount,
                  case
                    when dn.status in ('RESCHEDULED')
                    and oi.status in ('ITEM_RESCHEDULED') then oi.total_orderd - (oi.qty_delivered * oi.discount_amount)
                  else 0 end as dn_rescheduled_amount,
                  case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID', 'DRIVER_CANCELLED') and oi.status in ('ITEM_CANCELLED') then oi.total_orderd else 0 end as delivery_note_cancelled_amount,
                  case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.net_total_delivered else 0 end as gmv_vat_incl,
                  from delivery_notes dn, unnest(order_items) oi
                  left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'DISPATCHED') d on dn.id = d.id
                  left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'OPS_CANCELLED') oc on dn.id = oc.id
                  left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'USER_CANCELLED') uc on dn.id = uc.id
                  where index = 1
                  --and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                  --and oi.status = 'ITEM_FULFILLED'
                  ),
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
------------------------------ Delivery Trips -----------------------
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --where date(created_at) = current_date
                and status not in ('CANCELLED')
                and date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 month)
                and fulfillment_center_id = '0HEHY3146QXKF'
              ),
delivery_trips_cte as (
                        select distinct created_at,

                        country_code,
                        territory_id,
                        fulfillment_center_id as fulfilment_center_id,

                        id,
                        status,
                        dispatched_value,

                        delivery_note_ids as delivery_note_id
                        from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
                        where index = 1
                      ),
dt_agg_cte as (
                select distinct date(created_at) as date,
                territory_id,
                fulfilment_center_id,
                count(distinct id) as delivery_trip_count,
                sum(dispatched_value) as delivery_trip_dispatched_value
                from delivery_trips_cte, vars
                where date(created_at) between vars.current_start_date and vars.current_end_date
                group by 1,2,3
                ),
--------------------------- Mashup ----------------------
so_dn_and_dt_items_cte as (
                            select distinct date(soii.created_date) as so_creation_date,
                            
                            soii.country_code,
                            soii.territory_id,
                            soii.fulfilment_center_id,
                            soii.fulfilment_center_name,

                            soii.id as so_id,
                            soii.name as so_code,
                            soii.order_status as so_status,

                            soii.product_bundle_id,
                            soii.uom,
                            soii.fulfilment_status as so_item_fulfilment_status,
                            soii.net_total as so_amount,

                            date(dni.created_at) as dn_creation_date,
                            date(dni.dispatched_datetime) as dn_dispatched_date,
                            date(dni.delivery_date) as dn_delivery_date,
                            dni.id as dn_id,
                            dni.code as dn_code,
                            dni.status as dn_status,
                            dni.item_status as dn_item_status,
                            dni.dn_user_cancelled_amount,
                            dni.dn_ops_cancelled_amount,
                            dni.delivery_note_dispatched_amount as dn_dispatched_amount,
                            dni.dn_rescheduled_amount,
                            dni.delivery_note_cancelled_amount as dn_cancelled_amount,
                            dni.dn_rescheduled_amount + dni.delivery_note_cancelled_amount as dn_returned_amount,
                            dni.gmv_vat_incl,

                            date(dt.created_at) as dt_creation_date,
                            dni.delivery_trip_id as dt_id,
                            dt.status as dt_status,

                            # validate dates
                            date(soii.created_date) = date(dni.created_at) as check_so_and_dn_creation_date,
                            date(dni.dispatched_datetime) = date(dni.delivery_date) as check_dn_dispatch_and_delivered_dates
                            from so_inventory_items_cte soii
                            left join dn_items_cte dni on soii.id = dni.sale_order_id and soii.product_bundle_id = dni.product_bundle_id and soii.uom = dni.uom
                            left join delivery_trips_cte dt on dni.delivery_trip_id = dt.id and dni.id = dt.delivery_note_id

                            where soii.fulfilment_center_id = '0HEHY3146QXKF'
                            order by so_creation_date desc, so_id, product_bundle_id, uom
                            ),
so_dn_and_dt_cte as (
                    select distinct so_creation_date,
                    
                    country_code,
                    territory_id,
                    fulfilment_center_id,
                    fulfilment_center_name,

                    so_id,
                    so_code,
                    so_status,

                    dn_creation_date,
                    dn_dispatched_date,
                    dn_delivery_date,
                    dn_id,
                    dn_code,
                    dn_status,
                    sum(dn_dispatched_amount) as dn_dispatched_amount,
                    sum(dn_rescheduled_amount) as dn_rescheduled_amount,
                    sum(dn_cancelled_amount) as dn_cancelled_amount,
                    sum(dn_returned_amount) as dn_returned_amount,
                    sum(gmv_vat_incl) as gmv_vat_incl,
                    sum(dn_user_cancelled_amount) as dn_user_cancelled_amount,
                    sum(dn_ops_cancelled_amount) as dn_ops_cancelled_amount,

                    --sum(dn_settlement_cte.settlement_amount) as settlement_amount
                    --dn_settlement_cte.settlement_amount

                    # validate dates
                    check_so_and_dn_creation_date,
                    check_dn_dispatch_and_delivered_dates
                    from so_dn_and_dt_items_cte 
                    --left join dn_settlement_cte on so_dn_and_dt_items_cte.dn_id = dn_settlement_cte.id
                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14, 22,23
                    order by so_creation_date desc, so_id
                    ),
--*/
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
                        from so_agg_cte
                        full outer join dn_dispatch_agg_cte using(date, fulfilment_center_id)
                        full outer join dn_delivered_agg_cte using(date, fulfilment_center_id) --on soa.date = dna.date and soa.fulfilment_center_id = dna.fulfilment_center_id
                        full outer join dt_agg_cte using(date, fulfilment_center_id) 
                        full outer join dn_user_cancelled_agg_cte using(date, fulfilment_center_id) 
                        full outer join dn_ops_cancelled_agg_cte using(date, fulfilment_center_id) 
                        order by date desc
                        )
--select * from dn_settlement_cte
select * from so_dn_and_dt_cte where dn_id = '0HF8VVFEV7G6A'
--select * from so_dn_and_dt_items_cte
--select * from so_dn_and_dt_agg_cte