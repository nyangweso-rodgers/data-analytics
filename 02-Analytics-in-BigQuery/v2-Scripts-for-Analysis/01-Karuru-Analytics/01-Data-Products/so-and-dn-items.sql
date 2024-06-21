----------- SO & DN Items --------------------
with
sales_order as (
                SELECT *,
                row_number()over(partition by id  order by last_modified_date desc) as index
                FROM `kyosk-prod.karuru_reports.sales_order` so
                --where date(created_date) between '2023-08-01' and '2023-10-31'
                where date(created_date) >= date_sub(current_date, interval 1 month)
                --and is_pre_karuru = false
                ),
sales_order_item as (
                    select distinct date(created_date) as order_date,
                    --extract( day from date(created_date)) as get_day,
                    format_date('%A', date(created_date)) as get_sale_order_day,
                    so.id,
                    so.order_status,
                    so.territory_id,
                    --outlet.latitude,
                    --outlet.longitude,
                    so.outlet_id,
                    --i.fulfilment_status
                    --sum(i.total) as ordered_amount
                    from sales_order so, unnest(items) i
                    where index = 1
                    and so.order_status not in ('EXPIRED', 'USER_CANCELLED', 'SUBMITTED')
                    and so.territory.country_code = 'ng'
                    and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                    --group by 1,2,3,4,5
                    ),
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) > '2023-08-05'
                where date(created_at) >= date_sub(current_date, interval 1 month)
                --and is_pre_karuru = false
                ),
delivery_notes_items as (
                        select distinct 
                        id,
                        code,
                        sale_order_id,
                        dn.status,
                        date(dn.delivery_date) as delivery_date,
                        sum(total_delivered) as total_delivered
                        from delivery_notes dn, unnest(order_items) dni
                        where index = 1
                        and country_code = 'NG'
                        AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                        and dni.status = 'ITEM_FULFILLED'
                        group by 1,2,3,4,5
                        ),
so_and_dn_report as (
                      select distinct 
                      soi.territory_id,
                      soi.outlet_id,
                      count(distinct soi.order_date) as count_order_dates,
                      count(distinct soi.id) as count_sale_orders,
                      count(distinct soi.get_sale_order_day) as count_sale_order_week_days
                      
                      from sales_order_item soi
                      group by 1,2
                      )
select *
from so_and_dn_report
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')