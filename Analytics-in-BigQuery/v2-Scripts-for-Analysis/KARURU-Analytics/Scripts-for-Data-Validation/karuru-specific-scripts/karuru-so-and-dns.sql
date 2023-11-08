---------------------- KARURU ------------
----------- SO --------------------
with
karuru_so as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
              and date(created_date) between '2023-08-01' and '2023-10-31'
              --and date(created_date) >= date_sub(current_date, interval 4 month)
              and is_pre_karuru = false
              ),
so_summary as (
                select distinct date(created_date) as order_date,
                so.id as sales_order_id,
                so.order_status,
                so.territory_id,
                outlet.latitude,
                outlet.longitude,
                so.outlet_id,
                --i.fulfilment_status
                sum(i.total) as ordered_amount
                from karuru_so so, unnest(items) i
                where index = 1
                and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                and so.territory.country_code = 'ng'
                and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                group by 1,2,3,4,5,6,7
                ),
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-05'
                and is_pre_karuru = false
                ),
dns_items as (
              select distinct 
              id,
              code,
              sale_order_id,
              dn.status,
              date(dn.delivery_date) as delivery_date,
              sum(total_delivered) as total_delivered
              from karuru_dns dn, unnest(order_items) dni
              where index = 1
              and country_code = 'NG'
              AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
              and dni.status = 'ITEM_FULFILLED'
              group by 1,2,3,4,5
              )
select so.*,
dn.status as payment_status,
dn.delivery_date,
dn.total_delivered
from so_summary so
left join dns_items dn on so.sales_order_id = dn.sale_order_id