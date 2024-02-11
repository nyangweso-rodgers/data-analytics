---------------------- KARURU ------------
----------- SO --------------------
with
karuru_so as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
              --and date(created_date) between '2023-08-01' and '2024-02-03'
              --and date(created_date) between '2024-01-28' and '2024-02-03'
              --and date(created_date) >= date_sub(current_date, interval 2 month)
              and date(created_date) >= '2024-01-01'
              and is_pre_karuru = false
              ),
so_summary as (
                select distinct date(created_date) as created_date,
                so.id,
                so.name,
                so.order_status,
                so.territory_id,
                --so.outlet_id,
                so.created_on_app,
                so.market_developer_name,
                --so.created_by,
                --market_developer.id as market_developer_id,
                --market_developer.first_name,
                --market_developer.last_name,
                --market_developer.phone_number,
                i.fulfilment_status,
                --sum(i.total) as ordered_amount
                from karuru_so so, unnest(items) i
                where index = 1
                --and so.order_status not in ('INITIATED', 'EXPIRED', 'USER_CANCELLED')
                --and order_status in ('SUBMITTED', 'PROCESSING', 'DELIVERED', 'DISPATCHED', 'PUBLISHED')
                --and so.territory.country_code = 'ke'
                --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                --and name in ('SO2BOZJ2024')
                and market_developer_name in ('yvonne irungu')
                group by 1,2,3,4,5,6,7,8,9
                )
select distinct market_developer_name, created_on_app, order_status, fulfilment_status, sum(ordered_amount) as ordered_amount
from so_summary
--where (market_developer_id is null) or (first_name is null) or (last_name is null) or (phone_number is null)
order by 1,2