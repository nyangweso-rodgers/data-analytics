---------------------- KARURU ------------
----------- SO --------------------
with
sales_order as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
              --and date(created_date) between '2023-08-01' and '2024-02-03'
              --and date(created_date) between '2024-02-01' and '2024-04-20'
              --and date(created_date) >= date_sub(current_date, interval 2 month)
              and date(created_date) >= '2024-01-01'
              --and is_pre_karuru = false
              ),
so_report as (
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
                --i.fulfilment_status,
                --sum(i.total) as ordered_amount
                from sales_order so--, unnest(items) i
                where index = 1
                --and so.order_status not in ('INITIATED', 'EXPIRED', 'USER_CANCELLED')
                --and order_status in ('SUBMITTED', 'PROCESSING', 'DELIVERED', 'DISPATCHED', 'PUBLISHED')
                --and so.territory.country_code = 'ke'
                --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                and name in ('SOIHHVO2024')
                --and market_developer_name in ('yvonne irungu')
                --group by 1,2,3,4,5,6,7,8
                )
select * from so_report