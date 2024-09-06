--------------------- Delivery Note Item ---------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --where date(created_at) = current_date
                and date(created_at) > date_sub(current_date, interval 4 month)
                --where date(created_at) > date_sub(current_date, interval 30 day)
                --and is_pre_karuru = false
                --and date(created_at) between '2022-08-01' and '2022-11-30'
                --and country_code = 'KE'
                ),
delivery_notes_items as (
                          select distinct --date(created_at) as 
                          --dn.delivery_window.delivery_date as scheduled_delivery_date,
                          created_at,
                          updated_at,
                          bq_upload_time,
                          --date(delivery_date) as delivery_date,
                          --date_diff(date(updated_at), date(delivery_date), day) as check_delivery_date_diff,
                          coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                          country_code,
                          territory_id,
                          route_id,
                          route_name,
                          id,
                          code,
                          --dn.sale_order_id,
                          dn.status,
                          delivery_trip_id,
                          --payment_request_id,
                          agent_name as market_developer,
                          --outlet.phone_number,
                          outlet_id,
                          --outlet.name as outlet_name,
                          --outlet.outlet_code as outlet_code,
                          --outlet.latitude,
                          --outlet.longitude,
                          oi.product_bundle_id,
                          oi.uom,
                          oi.status as item_status,
                          oi.item_group_id,
                          --sum(oi.total_orderd) as total_orderd,
                          --sum(oi.total_delivered) as  
                          oi.total_delivered,
                          from delivery_notes dn, unnest(order_items) oi
                          where index = 1
                          and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                          and oi.status = 'ITEM_FULFILLED'
                          ),
latest_delivery_note as (
                          select distinct outlet_id,
                          LAST_VALUE(market_developer) OVER (PARTITION BY route_name ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as market_developer_name
                          from delivery_notes_items
                          ),
monthly_delivey_notes_items as (
                                select distinct --date_trunc(date(created_at), month) as created_at_month,
                                date_trunc(delivery_date, month) as deivery_month,
                                country_code,
                                --territory_id,
                                count(distinct id) as count_of_deliveries,
                                count(distinct outlet_id) as count_of_outlets,
                                sum(total_delivered) as gmv,
                                sum(total_delivered) / count(distinct id) as avg_basket_value
                                from delivery_notes_items
                                where delivery_date between '2024-07-01' and '2024-08-31'
                                group by 1,2
                                order by 1,2
                                )
select *
--distinct check_delivery_date_diff, count(distinct id)
--max(created_at), max(updated_at), max(bq_upload_time)
from monthly_delivey_notes_items
--and delivery_date is null
--and check_delivery_date_diff > 0
--where id = '0G4DPSFMYGDFS'
--where code = 'DN-KARA-0FWK97MDPQNST'
--and item_status in ('ITEM_REMOVED')
--and code in ('DN-KWMP-0GPRGEKQNRB4X')
--and FORMAT_DATE('%Y%m%d', scheduled_delivery_date) between @DS_START_DATE and @DS_END_DATE