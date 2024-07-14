--------------------- Delivery Note Item ---------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 month)
                --where date(created_at) > date_sub(current_date, interval 30 day)
                --and is_pre_karuru = false
                where date(created_at) between '2024-06-01' and '2024-06-30'
                ),
delivery_notes_items as (
                          select distinct --date(created_at) as 
                          created_at,
                          updated_at,
                          bq_upload_time,
                          date(delivery_date) as delivery_date,
                          date_diff(date(updated_at), date(delivery_date), day) as check_delivery_date_diff,
                          --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                          country_code,
                          territory_id,
                          --route_id,
                          --route_name,
                          id,
                          --code,
                          --dn.sale_order_id,
                          --dn.status,
                          --delivery_trip_id,
                          --payment_request_id,
                          --agent_name as market_developer,
                          --outlet.phone_number,
                          outlet_id,
                          --outlet.name as outlet_name,
                          --outlet.outlet_code as outlet_code,
                          --outlet.latitude,
                          --outlet.longitude,
                          oi.product_bundle_id,
                          oi.item_group_id,
                          LAST_VALUE(agent_name) OVER (PARTITION BY route_name ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as market_developer_name
                          from delivery_notes dn, unnest(order_items) oi
                          where index = 1
                          --and country_code = 'TZ'
                          --and territory_id in ('Vingunguti')
                          AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                          and oi.status = 'ITEM_FULFILLED'
                          ),
monthly_delivey_notes_items as (
                                select distinct date_trunc(date(created_at), month) as created_at_month,
                                count(distinct id) as count_id,
                                count(distinct outlet_id) as outlet_id
                                from delivery_notes_items
                                group by 1
                                )
select *
--distinct check_delivery_date_diff, count(distinct id)
--max(created_at), max(updated_at), max(bq_upload_time)
from monthly_delivey_notes_items
--where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
--and delivery_date is null
--and check_delivery_date_diff > 0
--where id = '0G4DPSFMYGDFS'
--where code = 'DN-KARA-0FWK97MDPQNST'