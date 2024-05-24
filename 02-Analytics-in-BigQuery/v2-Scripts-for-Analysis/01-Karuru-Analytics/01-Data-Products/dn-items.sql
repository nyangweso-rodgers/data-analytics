--------------------- Delivery Notes ---------------
------------------ DNs Items  ------------------------
with
delivery_notes as (
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index
                  FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                  --where date(created_at) > '2024-01-01'
                  where date(created_at) between '2023-10-01' and '2023-12-31'
                  ),
delivery_note_items as (
                        select distinct created_at,
                        --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                        country_code,
                        id,
                        --code,
                        --dn.status,
                        dn.outlet_id,
                        --outlet.name as outlet_name,
                        --outlet.phone_number,
                        --dn.agent_name as market_developer,
                        --oi.status as item_status,
                        dn.territory_id ,
                        oi.product_bundle_id,
                        --oi.uom,  
                        sum(oi.qty_delivered) as qty,
                        sum(oi.total_delivered) as total_delivered,
                        --sum(oi.total_delivered) / sum(oi.qty_delivered) as unit_price
                        from delivery_notes dn, unnest(order_items) oi
                        where index = 1
                        AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                        and oi.status = 'ITEM_FULFILLED'
                        group by 1,2,3,4,5,6
                        )
select *except(country_code),
--rank()over(partition by country_code order by product_bundle_id asc)
from delivery_note_items
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
and country_code = 'KE'
--and delivery_date between '2024-01-01' and '2024-03-31'
--and delivery_date = '2024-05-22'