
------------------ Delivery Notes Items, Inventory Items  ------------------------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --and date(created_at) > date_sub(current_date, interval 1 month)
                and date(created_at) between '2022-06-01' and '2022-07-30'
                --and status in ('PAID','DELIVERED','CASH_COLLECTED')
                --and date(created_at) > '2023-10-23'
                --and country_code = 'KE'
                --and territory_id = 'Voi'
                --and sale_order_code = 'SOZ5FLQ2024'
                ),
dn_items_cte as (
                  select distinct coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                  country_code,
                  dn.territory_id,

                  dn.outlet_id,
                  dn.delivery_trip_id,
                  dn.id,
                  dn.code,
                  dn.sale_order_id,
                  dn.sale_order_code,

                  oi.item_group_id,
                  oi.product_bundle_id, 
                  oi.uom,
                  oi.inventory_items,
                  oi.catalog_item_qty,
                  oi.qty_delivered,
                  oi.total_delivered,
                  oi.net_total_delivered,
                  --sum(oi.net_total_delivered) as gmv_vat_incl,
                  --sum(oi.catalog_item_qty) as catalog_item_qty,
                  --sum(oi.qty_delivered) as qty_delivered 
                  from delivery_notes dn, unnest(order_items) oi
                  where index = 1
                  --and oi.status = 'ITEM_FULFILLED'
                  ),
dn_inventory_items_cte as (
                          select distinct  dni.delivery_date,
                          dni.country_code,
                          dni.territory_id,
                          dni.outlet_id,

                          dni.delivery_trip_id,
                          dni.id,
                          dni.code,
                          dni.sale_order_id,
                          dni.sale_order_code,

                          dni.item_group_id,
                          dni.product_bundle_id,
                          dni.uom,
                          --sum(dni.catalog_item_qty) as catalog_item_qty,
                          --sum(dni.qty_delivered) as qty_delivered,
                          --sum(dni.gmv_vat_incl) as gmv_vat_incl,
                          dni.catalog_item_qty,
                          dni.qty_delivered,
                          dni.total_delivered,
                          dni.net_total_delivered,

                          sum(ii.conversion_factor) as conversion_factor,
                          string_agg(distinct ii.stock_item_id ,"/" order by stock_item_id) as stock_item_id,
                          string_agg(distinct ii.uom,"/" order by ii.uom) as stock_uom,
                          sum(ii.inventory_item_qty) as inventory_item_qty,

                          --string_agg(distinct ii.dimension.metric, "/" order by ii.dimension.metric) as dimension_metric,
                          --string_agg(distinct cast(ii.dimension.length as string), "/" order by cast(ii.dimension.length as string)) as dimension_length
                          from dn_items_cte dni, unnest(inventory_items) ii 
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
                          ),
dns_inventory_items_agg_cte as (
                                select distinct country_code,
                                count(distinct outlet_id) as outlets_count,
                                count(distinct id) as dns_count,
                                sum(net_total_delivered) as net_total_delivered
                                from dn_inventory_items_cte
                                where delivery_date between '2024-09-01' and '2024-09-30'
                                group by 1
                                )
select * from dn_inventory_items_cte
--where id = '0FJZ2953HGACE'
where delivery_trip_id = '0D38GG700MFQ9'