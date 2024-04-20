----------- Karuru ----------
---------- DNs and SO ------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --and date(created_at) > '2023-08-05'
                --and date(created_at) > '2023-08-01'
                and date(created_at) between '2024-01-29' and '2024-02-03'
                and is_pre_karuru = false
                ),
dns_report as (
              select distinct date(created_at) as created_at,
              --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
              --country_code,
              --territory_id,
              id,
              --code,
              dn.sale_order_id,
              dn.status,
              --agent_name as market_developer,
              --outlet.phone_number,
              --outlet_id,
              --outlet.name as outlet_name,
              --outlet.outlet_code as outlet_code,
              from karuru_dns dn
              where index = 1
              --and country_code = 'TZ'
              --and territory_id in ('Vingunguti')
              --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
              --and dn.status = 'DRIVER_CANCELLED'
              --and dni.status = 'ITEM_FULFILLED'
              ),
karuru_so as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              --where date(created_date) between '2023-08-01' and '2024-01-29'
              --and date(created_date) between '2024-01-28' and '2024-02-03'
              where date(created_date) >= date_sub(current_date, interval 2 month)
              --and date(created_date) = '2024-01-29'
              and is_pre_karuru = false
              ),
so_report as (
                select distinct date(created_date) as created_date,
                so.id,
                --so.name,
                so.order_status,
                --so.territory_id,
                --so.outlet_id,
                so.created_on_app,
                --so.market_developer_name,
                --so.created_by,
                --market_developer.id as market_developer_id,
                --market_developer.first_name,
                --market_developer.last_name,
                --market_developer.phone_number,
                --i.fulfilment_status,
                --sum(i.total) as ordered_amount
                from karuru_so so--, unnest(items) i
                where index = 1
                --and so.order_status not in ('INITIATED', 'EXPIRED', 'USER_CANCELLED')
                --and so.order_status = 'USER_CANCELLED'
                --and so.territory.country_code = 'ke'
                --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                --and name in ('SO2BOZJ2024')
                --and market_developer_name in ('yvonne irungu')
                --group by 1,2,3,4,5,6,7,8,9
                )
select dn.*, so.order_status, so.created_on_app
from dns_report dn
left join so_report so on dn.sale_order_id = so.id
where order_status = 'SUBMITTED'