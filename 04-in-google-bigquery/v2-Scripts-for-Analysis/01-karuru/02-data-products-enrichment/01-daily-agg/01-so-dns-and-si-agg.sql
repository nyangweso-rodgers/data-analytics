with    
------------------------ Date Variabes ------------------------------
vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE)  as current_end_date ),
  --SELECT DATE '2024-11-25' as current_start_date,  DATE '2024-11-27' as current_end_date ),
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
so_cte as (
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
            --and id = 'SO-0HNE8EXM9A0Z2'
            ),
so_items_cte as (
                select distinct so.created_date,

                --so.last_modified_date,
                --so.bq_upload_time,

                so.territory.country_code as country_code,
                so.territory_id,
                so.route_id,
                so.route.route_name,
                --so.route.id as route_id,
                --so.route.route_name as route_name,

                --so.outlet_id,
                so.id,
                --so.name,
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
                --i.inventory_items,
                --i.promotion_type,
                --i.promotion_on,
                --i.discount_type
                from so_cte so, unnest(items) i
                where index = 1
                ),
so_agg_cte as (
                select distinct date(created_date) as date,
                country_code,
                territory_id,
                route_id,
                route_name,
                count(distinct id) as sales_order_count,
                sum(net_total) as sales_order_amount
                from so_items_cte, vars
                where date(created_date) between vars.current_start_date and vars.current_end_date
                group by 1,2,3,4,5
                ),
------------------------------ Delivery Notes ------------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                where date(created_at) > date_sub(current_date, interval 1 month)
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
dn_items_cte as (
                  select distinct dn.created_at,
                  d.change_time as dispatched_datetime,
                  oc.change_time as ops_cancelled_datetime,
                  uc.change_time as user_cancelled_datetime,
                  coalesce(date(delivery_date), date(updated_at)) as delivery_date,

                  --country_code,
                  territory_id,
                  --dn.fullfilment_center_id as fulfilment_center_id,
                  route_id,
                  route_name,

                  dn.id,
                  --dn.code,
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
                        route_id,
                        --route_name,
                        --fulfilment_center_id,
                        sum(delivery_note_dispatched_amount) as delivery_note_dispatched_amount
                        from dn_items_cte, vars
                        where date(dispatched_datetime) between vars.current_start_date and vars.current_end_date 
                        group by 1,2,3
                        ),
dn_user_cancelled_agg_cte as (
                        select distinct date(user_cancelled_datetime) as date,
                        territory_id,
                        route_id,
                        sum(dn_user_cancelled_amount) as dn_user_cancelled_amount
                        from dn_items_cte, vars
                        where date(user_cancelled_datetime) between vars.current_start_date and vars.current_end_date 
                        group by 1,2,3
                        ),
dn_ops_cancelled_agg_cte as (
                        select distinct date(ops_cancelled_datetime) as date,
                        territory_id,
                        route_id,
                        sum(dn_ops_cancelled_amount) as dn_ops_cancelled_amount
                        from dn_items_cte, vars
                        where date(ops_cancelled_datetime) between vars.current_start_date and vars.current_end_date 
                        group by 1,2,3
                        ),
dn_delivered_agg_cte as (
                      select distinct date(delivery_date) as date,
                      territory_id,
                      route_id,
                      count(distinct case when status in ('PAID','DELIVERED','CASH_COLLECTED') then id else null end) as delivery_note_count,
                      sum(delivery_note_cancelled_amount) + sum(dn_rescheduled_amount) as delivery_note_returned_amount,
                      sum(gmv_vat_incl) as gmv_vat_incl,
                      from dn_items_cte, vars
                      where date(delivery_date) between vars.current_start_date and vars.current_end_date 
                      group by 1,2,3
                      ),
----------- front margins ----------------------
scheduled_query_front_margin_report_cte as (
                      select distinct 
                      fm.delivery_date,
                      fm.territory_id,
                      fm.route_id,
                      --fm.route_name,
                      sum(fm.base_net_amount) as base_net_amount,
                      sum(fm.total_incoming_rate) as total_incoming_rate,
                      from `kyosk-prod.karuru_scheduled_queries.front_margin` fm
                      where fm.delivery_date >= date_sub(date_trunc(current_date, month), interval 2 month)
                      --where fm.delivery_date >= '2023-08-01'
                      --and fm.company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                      group by 1,2,3
                      ),
front_margins_agg_cte as (
                          select distinct date(delivery_date) as date,
                          territory_id,
                          route_id,
                          sum(base_net_amount) as base_net_amount,
                          sum(total_incoming_rate) as total_incoming_rate
                          from scheduled_query_front_margin_report_cte, vars
                          where date(delivery_date) between vars.current_start_date and vars.current_end_date 
                          group by 1,2,3
                          ),
---------------------- mashup ---------------------------
so_dn_and_dt_agg_cte as (
                        select so_agg_cte.*except(territory_id), 
                        uploaded_regional_mapping_cte.new_territory_id as territory_id,
                        uploaded_zero_md_routes_cte.zero_md_route,

                        coalesce(dn_dispatch_agg_cte.delivery_note_dispatched_amount,0) as delivery_note_dispatched_amount,
                        coalesce(dn_delivered_agg_cte.delivery_note_count, 0) as delivery_note_count,
                        coalesce(dn_delivered_agg_cte.delivery_note_returned_amount, 0) as delivery_note_returned_amount,
                        coalesce(dn_delivered_agg_cte.gmv_vat_incl, 0) as gmv_vat_incl,
                        coalesce(safe_divide(dn_delivered_agg_cte.gmv_vat_incl , dn_delivered_agg_cte.delivery_note_count),0) as delivery_note_avg_basket_value,

                        --coalesce(dt_agg_cte.delivery_trip_count, 0) as delivery_trip_count,

                        coalesce(dn_user_cancelled_agg_cte.dn_user_cancelled_amount, 0) as dn_user_cancelled_amount,
                        coalesce(dn_ops_cancelled_agg_cte.dn_ops_cancelled_amount, 0) as dn_ops_cancelled_amount,

                        coalesce(front_margins_agg_cte.base_net_amount, 0) as base_net_amount,
                        coalesce(front_margins_agg_cte.total_incoming_rate, 0) as total_incoming_rate
                        from so_agg_cte
                        left join uploaded_regional_mapping_cte on so_agg_cte.territory_id = uploaded_regional_mapping_cte.original_territory_id
                        left join uploaded_zero_md_routes_cte on so_agg_cte.route_id = uploaded_zero_md_routes_cte.route_id
                        full outer join dn_delivered_agg_cte on so_agg_cte.route_id = dn_delivered_agg_cte.route_id and so_agg_cte.date = dn_delivered_agg_cte.date
                        full outer join dn_dispatch_agg_cte on so_agg_cte.route_id = dn_dispatch_agg_cte.route_id and so_agg_cte.date = dn_dispatch_agg_cte.date
                        
                        --full outer join dt_agg_cte 
                        full outer join dn_user_cancelled_agg_cte on so_agg_cte.route_id = dn_user_cancelled_agg_cte.route_id and so_agg_cte.date = dn_user_cancelled_agg_cte.date
                        full outer join dn_ops_cancelled_agg_cte on so_agg_cte.route_id = dn_ops_cancelled_agg_cte.route_id and so_agg_cte.date = dn_ops_cancelled_agg_cte.date
                        full outer join front_margins_agg_cte on so_agg_cte.route_id = front_margins_agg_cte.route_id and so_agg_cte.date = front_margins_agg_cte.date
                        order by date desc
                        )

select * from so_dn_and_dt_agg_cte
where zero_md_route = true