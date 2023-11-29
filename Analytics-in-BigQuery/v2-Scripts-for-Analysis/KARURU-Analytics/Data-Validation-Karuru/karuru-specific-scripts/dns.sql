--------------------- Karuru ---------------
------------------ DNs Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-05'
                and is_pre_karuru = false
                ),
dns_list as (
              select distinct --date(created_at) as created_at,
              -- coalesce(date(delivery_date), date(updated_at)) as delivery_date,
              --country_code,
              id,
              --code,
              --dn.sale_order_id,
              dn.status,
              from karuru_dns dn
              where index = 1
              --and country_code = 'TZ'
              --AND dn.status not IN ('PAID', 'DELIVERED')
              --and dni.status = 'ITEM_FULFILLED'
              )
select *
from dns_list
where id in ()