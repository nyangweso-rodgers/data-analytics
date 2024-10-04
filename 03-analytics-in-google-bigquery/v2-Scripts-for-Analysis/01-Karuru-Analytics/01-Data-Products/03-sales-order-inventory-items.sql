with    
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                and territory.country_code = 'ke'
                --where territory_id = 'Ruiru'
                --and date(created_date) between '2024-09-01' and '2024-09-12' 
                and date(created_date) >= date_sub(current_date, interval 1 week)
                and name = 'SO8GD9P2024'
                ),
sales_order_items_cte as (
                          select distinct so.created_date,
                          so.last_modified_date,
                          so.bq_upload_time,

                          so.territory.country_code as country_code,
                          so.territory_id,
                          so.route.id as route_id,
                          so.route.route_name as route_name,

                          so.outlet_id,
                          so.id,
                          so.name,
                          so.created_on_app,
                          so.market_developer.id as market_developer_id,
                          so.market_developer_name,
                          so.order_status,

                          i.product_bundle_id,
                          i.uom,
                          i.fulfilment_status,
                          i.catalog_item_qty,
                          i.selling_price,
                          i.net_total,
                          i.discount_amount,
                          i.inventory_items,
                          i.promotion_type,
                          i.promotion_on,
                          i.discount_type
                          from sales_order so, unnest(items) i
                          where index = 1
                          ),
sales_order_inventory_items_cte as (
                                    select distinct soi.created_date,
                                    soi.last_modified_date,
                                    soi.bq_upload_time,

                                    soi.country_code,
                                    soi.territory_id,
                                    ii.fulfilment_center_id,
                                    ii.fulfilment_center_name,
                                    soi.route_id,
                                    soi.route_name,

                                    soi.outlet_id,
                                    soi.id,
                                    soi.name,
                                    soi.created_on_app,
                                    soi.order_status,

                                    soi.market_developer_id,
                                    soi.market_developer_name,

                                    soi.product_bundle_id,
                                    soi.uom,
                                    soi.fulfilment_status,
                                    soi.catalog_item_qty,
                                    soi.selling_price,
                                    soi.net_total,
                                    soi.discount_amount,
                                    ii.conversion_factor,
                                    ii.stock_item_id,
                                    ii.stock_uom,
                                    ii.inventory_item_qty,
                                    soi.promotion_type,
                                    soi.promotion_on,
                                    soi.discount_type
                                    from sales_order_items_cte soi, unnest(inventory_items) ii
                                    )
select * from sales_order_inventory_items_cte