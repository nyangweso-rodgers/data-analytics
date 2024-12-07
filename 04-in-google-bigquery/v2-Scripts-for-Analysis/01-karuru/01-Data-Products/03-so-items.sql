----------- sales order item --------------------
with
------------------------- Upload - Customer Sales Order Cancellations Risks ---------------------------------
uploaded_outlets_cancellations_risks as (
                                          SELECT distinct territory_id, 
                                          outlet_id, 
                                          order_cancellation_risk 
                                          FROM `kyosk-prod.karuru_test.sales_order_cancellations_risks` 
                                          ),
--------------------------------- Upload - Item Group Type ------------------------
item_group_mapping as (
                        SELECT distinct country_code,
                        item_group_id,
                        type
                        FROM `kyosk-prod.karuru_upload_tables.item_group_mapping` 
                        where country_code = 'KE'
                        ),
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                --and territory.country_code = 'ke'
                --where territory_id = 'Ruiru'
                --and date(created_date) between '2024-09-01' and '2024-09-12' 
                and date(created_date) >= date_sub(current_date, interval 1 week)
                --and name = 'SO8GD9P2024'
                ),
so_item_cte as (
                select distinct  created_date,
                last_modified_date,
                bq_upload_time,
                --extract( day from date(created_date)) as get_day,
                --format_date('%A', date(created_date)) as get_sale_order_day, 
                so.delivery_window.id as delivery_window_id,
                so.delivery_window.delivery_date as scheduled_delivery_date,
                so.delivery_window.start_time as deliveey_window_start_time,
                so.delivery_window.end_time as deliveey_window_end_time,

                so.territory.country_code as country_code,
                so.territory_id,
                so.route.id as route_id,
                so.route.route_name as route_name,

                so.id,
                so.name,
                so.created_on_app,
                so.order_status,

                so.market_developer.id as market_developer_id,
                so.market_developer_name,

                so.outlet_id,
                cast(outlet.latitude as float64) as latitude,
                cast(outlet.longitude as float64) as longitude,

                i.category_id,
                case
                  when (igm.type is not null) then igm.type
                  when (igm.type is null) and (i.category_id in ('Flour', 'Four', 'Cooking Oil & Fats', 'Sugar')) then 'Revenue Movers'
                  when (igm.type is null) and (i.category_id in ('Home Care', 'Beauty.', 'Personal Care', 'Condiment', 'Dairy', 'Beverage', 'Cereals', 'Health', 'Baby Care')) then 'Margin Movers'
                  when (igm.type is null) and (i.category_id in ('Stationary')) then 'New Category'
                else null end as item_group_type,
                i.product_bundle_id,
                i.uom,
                i.fulfilment_status,
                i.catalog_item_qty,
                i.selling_price,
                i.net_total
                from sales_order so, unnest(items) i
                left join item_group_mapping igm on i.category_id = igm.item_group_id
                where index = 1
                --and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                --group by 1,2,3,4,5
                ),
so_item_agg_cte as (
                    select distinct date(created_date) as so_creation_date,
                    territory_id,
                    count(distinct outlet_id) as outlet_count,
                    count(distinct id) as so_count,
                    from so_item_cte
                    where date(created_date) = '2024-12-02' and territory_id = 'Ruiru'
                    group by 1,2
                    )
select *
--max(created_date) as max_created_date, max(last_modified_date) as max_last_modified_date, max(bq_upload_time) as max_bq_upload_time
from so_item_agg_cte 