-------------------- Karuru -------------------
------------------- Monthly Paid & Delived Dns -----------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                and status in ('DELIVERED', 'PAID')
                and date(created_at) = '2022-08-12'
                --and date(delivery_date) between '2022-08-01' and '2022-08-31'
                --and is_pre_karuru = true
                --and country_code = 'TZ'
                ),
monthly_lists as (
                  select distinct country_code,
                  date(delivery_date) as delivery_date,
                  code,
                  dn.status,
                  is_pre_karuru,
                  dni.product_bundle_id,
                  dni.status as item_status,
                  dni.total_orderd,
                  dni.total_delivered
                  from karuru_dns dn, unnest(order_items) dni
                  where index = 1
                  --and dni.status = 'ITEM_FULFILLED'
                  and code = "DN-VING-RXDH"
                ),
monthly_agg as (
                select distinct country_code,
                date_trunc(date(delivery_date), month) as delivery_month,
                count(distinct code) as codes,
                count(distinct outlet_id) as outlet_id,
                sum(amount) as amount
                from karuru_dns dn
                where index = 1 
                group by 1,2
                order by 2
                ),
monthly_agg_items as (
                      select distinct country_code,
                      date_trunc(date(delivery_date), month) as delivery_month,
                      count(distinct code) as codes,
                      count(distinct outlet_id) as outlet_id,
                      sum(total_delivered) as total_delivered
                      from karuru_dns dn, unnest(order_items) dni
                      where index = 1
                      and dni.status = 'ITEM_FULFILLED'
                      group by 1,2
                      order by 2
                      )
select *
from monthly_lists 