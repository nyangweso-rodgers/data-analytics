----------- SO  Items --------------------
with
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where date(created_date) between '2023-08-01' and '2023-10-31'
                --where date(created_date) >= date_sub(current_date, interval 1 month)
                --and is_pre_karuru = false
                where date(created_date) between '2024-06-24' and '2024-06-28'
                --where date(created_date) = '2024-06-10'
                ),
sales_order_item as (
                    select distinct date(created_date) as created_date,
                    created_date as created_datetime,
                    so.territory.country_id,
                    so.territory.country_code,
                    so.territory_id,
                    --outlet.latitude,
                    --outlet.longitude,
                    so.outlet_id,
                    so.id,
                    so.created_on_app,
                    so.market_developer_name,
                    so.created_by,
                    so.order_status,
                    --i.fulfilment_status
                    --sum(i.total) as ordered_amount
                    i.product_bundle_id,
                    --sum(i.catalog_item_qty) as catalog_item_qty,
                    --sum(i.total) as total,
                    --sum(i.net_total) as net_total,
                    --avg(i.selling_price) as selling_price
                    from sales_order so, unnest(items) i
                    where index = 1
                    --and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                    
                    --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                    --group by 1,2,3
                    ),
sales_order_basket as (
                        select distinct id,
                        count(distinct product_bundle_id) as product_bundle_id
                        from sales_order_item
                        group by 1
                        ),
outlets_with_multiple_orders as (
                                  select distinct 
                                  country_id,
                                  territory_id,
                                  created_date,
                                  lag(created_datetime)over(partition by created_date, outlet_id order by created_datetime asc) as previous_created_datetime,
                                  created_datetime,
                                  datetime_diff(created_datetime, lag(created_datetime)over(partition by created_date, outlet_id order by created_datetime asc), minute) as created_datetime_delta,
                                  row_number()over(partition by created_date, outlet_id order by created_datetime asc) as order_id_index,
                                  outlet_id,
                                  id,
                                  created_on_app,
                                  market_developer_name,
                                  created_by,
                                  order_status,
                                  from sales_order_item
                                  --order by outlet_id, created_date, order_id_index
                                  ),
analysis_report as (
                    select owmo.*,
                    lag(sob.product_bundle_id)over(partition by created_date, outlet_id order by created_datetime asc) as previous_count_sku,
                    sob.product_bundle_id as count_sku,
                    sob.product_bundle_id - lag(sob.product_bundle_id)over(partition by created_date, outlet_id order by created_datetime asc) as sku_delta
                    from outlets_with_multiple_orders owmo
                    left join sales_order_basket sob on sob.id = owmo.id
                    order by outlet_id, created_date, order_id_index
                    )
select * 
from analysis_report
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
and country_id = 'Kenya'
--country_code = 'ke'
--and outlet_id in ('0CW5YAXE5TA2F', '0CW5YD12XHJJA', '0CW610NQWRN4W', '0CW612RA45RRY')
--and outlet_id = 'SO-0GA4HBX1JAK01'
--and created_by in ('salehshifa100@gmail.com')