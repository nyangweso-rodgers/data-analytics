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
                so.retailer_id,
                --i.fulfilment_status
                sum(i.total) as ordered_amount
                from karuru_so so, unnest(items) i
                where index = 1
                and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                and so.territory.country_code = 'ng'
                and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                group by 1,2,3,4,5,6,7
                )
select *
from so_summary
order by 1,2