---------------------- Sales Order Cancellations ----------------
----------- SO & DN Items --------------------
with
------------------------------- Sales Order ----------------------
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                where territory_id in ('Ruiru')
                and territory.country_code = 'ke'
                --where date(created_date) = '2024-06-01' 
                and date(created_date) >= date_sub(current_date, interval 3 month)
                --and is_pre_karuru = false
                --where date(created_date) between '2022-04-01' and '2024-06-30'
                --and date(created_date) between '2024-09-02' and '2024-09-10'
                ),
sales_order_item_cte as (
                    select distinct  created_date,
                    --extract( day from date(created_date)) as get_day,
                    --format_date('%A', date(created_date)) as get_sale_order_day,
                    so.territory.country_code as country_code,
                    so.territory_id,
                    so.route.id as route_id,
                    so.route.route_name as route_name,

                    so.id,
                    so.name,
                    so.created_on_app,
                    so.order_status,

                    --so.market_developer.id as market_developer_id,
                    --so.market_developer_name,

                    so.outlet_id,
                    cast(outlet.latitude as float64) as latitude,
                    cast(outlet.longitude as float64) as longitude,
                    
                    --i.product_bundle_id,
                    --i.uom,
                    --i.fulfilment_status,
                    --i.net_total
                    from sales_order so, unnest(items) i
                    where index = 1
                    --and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                    --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                    --group by 1,2,3,4,5
                    ),
sales_order_cancellatios_cte as (
                            select distinct territory_id,
                            outlet_id, 
                            count(distinct id) as sale_orders_count,
                            count(distinct(case when order_status in ('USER_CANCELLED', 'OPS_CANCELLED', 'CANCELLED', 'EXPIRED') then id else null end)) as cancelled_orders_count,
                            round(count(distinct(case when order_status in ('USER_CANCELLED', 'OPS_CANCELLED', 'CANCELLED', 'EXPIRED') then id else null end)) / count(distinct id),2) as cancelled_orders_percent
                            --count(distinct(case when order_status in ('USER_CANCELLED') then id else null end)) as users_cancelled_orders_count,
                            --count(distinct(case when order_status in ('OPS_CANCELLED') then id else null end)) as ops_cancelled_orders_count,
                            --count(distinct(case when order_status in ('CANCELLED') then id else null end)) as cancelled_orders_count,
                            --count(distinct(case when order_status in ('EXPIRED') then id else null end)) as expired_orders_count
                            from sales_order_item_cte
                            group by 1,2
                            ),
outlets_cancellation_risk_segment as (
                            select *,
                            case
                              when (cancelled_orders_percent <= 0.3) then 'LOW'
                              when (cancelled_orders_percent <= 0.6) then 'MEDIUM'
                              when (cancelled_orders_percent <= 1) then 'HIGH'
                            else 'UNSET' end as order_cancellation_risk
                            from sales_order_cancellatios_cte
                            )
select * from outlets_cancellation_risk_segment