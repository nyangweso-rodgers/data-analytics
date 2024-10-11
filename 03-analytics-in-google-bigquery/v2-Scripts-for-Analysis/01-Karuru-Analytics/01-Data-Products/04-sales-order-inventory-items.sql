with    
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                and order_status not in ('INITIATED')
                and territory.country_code = 'ke'
                --where territory_id = 'Ruiru'
                --and date(created_date) between '2024-09-01' and '2024-09-12' 
                and date(created_date) >= date_sub(current_date, interval 4 month)
                --and name = 'SO8GD9P2024'
                --and name = 'SOWZIST2024'
                --and id = 'SO-0HFMZ8MS02PWM'
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

                          i.category_id,
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

                                    
                                    soi.category_id,soi.product_bundle_id,
                                    soi.uom,
                                    soi.fulfilment_status,
                                    soi.catalog_item_qty,
                                    soi.selling_price,
                                    soi.net_total,
                                    soi.discount_amount,
                                    string_agg(distinct cast(ii.conversion_factor as string), "/" order by cast(ii.conversion_factor as string)) as conversion_factor,
                                    string_agg(distinct ii.stock_item_id, "/" order by ii.stock_item_id) as stock_item_id,
                                    string_agg(distinct ii.stock_uom, "/" order by ii.stock_uom) as stock_uom,
                                    sum(ii.inventory_item_qty) as inventory_item_qty,
                                    soi.promotion_type,
                                    soi.promotion_on,
                                    soi.discount_type
                                    from sales_order_items_cte soi, unnest(inventory_items) ii
                                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,29,30,31
                                    ),
sales_order_inventory_items_summary_cte as (
                                            select distinct country_code, 
                                            --id,
                                            --count(distinct outlet_id) as outlet_count, 
                                            --count(distinct id) as sale_order_count, 
                                            sum(net_total) as net_total
                                            from sales_order_inventory_items_cte
                                            where date(created_date) between '2024-09-01' and '2024-09-30'
                                            group by 1
                                            ),
sales_order_items_summary_cte as (
                                  select distinct country_code, 
                                  id,
                                  --count(distinct outlet_id) as outlet_count, 
                                  --count(distinct id) as sale_order_count, 
                                  sum(net_total) as net_total
                                  from sales_order_items_cte
                                  where date(created_date) between '2024-09-01' and '2024-09-30'
                                  group by 1,2
                                  )/*,
check_catalog_and_inventory_variance_cte as (
                                              select distinct i.id,
                                              i.net_total - ii.net_total as net_total_variance
                                              from sales_order_items_summary_cte i
                                              left join sales_order_inventory_items_summary_cte ii on i.id = ii.id
                                              )*/
--select * from check_catalog_and_inventory_variance_cte where net_total_variance <> 0
select distinct promotion_type, promotion_on from sales_order_inventory_items_cte order by 1,2