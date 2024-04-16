--------------------- Karuru ---------------
------------------ DNs  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-01-01'
                --and is_pre_karuru = false
                ),
dns_list as (
              select distinct 
              coalesce(date(delivery_date), date(updated_at)) as delivery_date,
              country_code,
              dn.territory_id,
              dn.outlet_id,
              dn.agent_name,
              from karuru_dns dn
              where index = 1
              and country_code = 'KE'
              AND dn.status not IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
              --and dni.status = 'ITEM_FULFILLED'
              ),
karuru_outlets as (
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index
                  FROM `kyosk-prod.karuru_reports.outlets` 
                  WHERE date(created_at) > '2022-01-01'
                  ),
outlets_lists as (
                        SELECT distinct 
                        id,
                        name,
                        phone_number,
                        FROM karuru_outlets--.market
                        --left join unnest(market) as market
                        WHERE index = 1
                        )
select DISTINCT country_code, 
outlet_id,
o.name,
o.phone_number,
last_value(territory_id) over(partition by outlet_id order by delivery_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as territory_id,
last_value(agent_name) over(partition by outlet_id order by delivery_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as agent_name,
from dns_list dn
left join outlets_lists o on dn.outlet_id = o.id
where delivery_date >= date_sub(current_date, interval 6 month)