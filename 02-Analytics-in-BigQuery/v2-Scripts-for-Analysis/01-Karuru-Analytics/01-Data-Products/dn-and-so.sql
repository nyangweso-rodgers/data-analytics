----------- Karuru ----------
---------- DNs and SO ------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --and date(created_at) > '2023-08-05'
                and date(created_at)  >= date_sub(current_date, interval 2 month)
                --and date(created_at) between '2024-01-29' and '2024-02-03'
                ),
dn_report as (
              select distinct date(created_at) as dn_created_at,
              --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
              --country_code,
              --territory_id,
              id as dn_id,
              --code,
              dn.sale_order_id,
              dn.status as dn_status,
              --agent_name as market_developer,
              --outlet.phone_number,
              --outlet_id,
              --outlet.name as outlet_name,
              --outlet.outlet_code as outlet_code,
              from delivery_notes dn
              where index = 1
              --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
              --and dn.status = 'DRIVER_CANCELLED'
              --and dni.status = 'ITEM_FULFILLED'
              ),
sales_order as (
              SELECT *,
              row_number()over(partition by id  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              --where date(created_date) between '2023-08-01' and '2024-01-29'
              --and date(created_date) between '2024-01-28' and '2024-02-03'
              where date(created_date) >= date_sub(current_date, interval 2 month)
              --and date(created_date) = '2024-01-29'
              ),
so_report as (
                select distinct date(created_date) as so_created_date,
                so.id,
                so.name as so_name,
                so.order_status as so_status,
                so.territory_id,
                so.outlet_id,
                so.created_on_app,
                --so.market_developer_name,
                so.created_by,
                market_developer.id as market_developer_id,
                market_developer.first_name,
                market_developer.last_name,
                market_developer.phone_number,
                --i.fulfilment_status,
                --sum(i.total) as ordered_amount
                from sales_order so--, unnest(items) i
                where index = 1
                --and so.order_status not in ('INITIATED', 'EXPIRED', 'USER_CANCELLED')
                --and so.order_status = 'USER_CANCELLED'
                --and so.territory.country_code = 'ke'
                --and i.fulfilment_status not in ('ITEM_EXPIRED', 'ITEM_REMOVED')
                --and market_developer_name in ('yvonne irungu')
                --group by 1,2,3,4,5,6,7,8,9
                ),
dn_so_report as (
                  select dn.*, so.*except(id)
                  from dn_report dn
                  left join so_report so on dn.sale_order_id = so.id
                  )
select * from dn_so_report
where so_name in ('SOIHHVO2024')