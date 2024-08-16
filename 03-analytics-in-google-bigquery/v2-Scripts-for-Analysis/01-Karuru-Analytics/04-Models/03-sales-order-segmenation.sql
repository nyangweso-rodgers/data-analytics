----------- Sale Order Items --------------------
with
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                --where date(created_date) between '2023-08-01' and '2023-10-31'
                --where date(created_date) >= date_sub(current_date, interval 1 month)
                --and is_pre_karuru = false
                and date(created_date) between '2024-06-01' and '2024-06-30'
                --where date(created_date) = '2024-06-10'
                --and territory.country_id = 'Kenya'
                and territory_id = 'Kisumu1'
                ),
sales_order_item as (
                    select distinct date(created_date) as sales_order_created_date,
                    so.territory.country_id,
                    so.territory_id,
                    outlet.latitude as outlet_latitude,
                    outlet.longitude as outlet_longitude,
                    so.outlet_id,
                    so.id,
                    --so.created_on_app,
                    --so.market_developer_name,
                    --so.created_by,
                    so.order_status,
                    --i.fulfilment_status
                    --sum(i.total) as ordered_amount
                    i.category_id,
                    i.product_bundle_id,
                    i.uom,
                    --sum(i.catalog_item_qty) as catalog_item_qty,
                    --sum(i.total) as total,
                    i.net_total,
                    --avg(i.selling_price) as selling_price
                    from sales_order so, unnest(items) i
                    where index = 1
                    --and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
          
                    --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                    --group by 1,2,3
                    )
select * 
from sales_order_item
--country_code = 'ke'
--and outlet_id in ('0CW5YAXE5TA2F', '0CW5YD12XHJJA', '0CW610NQWRN4W', '0CW612RA45RRY')
--and outlet_id = 'SO-0GA4HBX1JAK01'
--and created_by in ('salehshifa100@gmail.com')