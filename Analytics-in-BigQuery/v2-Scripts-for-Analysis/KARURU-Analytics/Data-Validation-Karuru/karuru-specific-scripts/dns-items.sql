--------------------- Karuru ---------------
------------------ Revenue - DNs Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2022-02-01'
                and is_pre_karuru = false
                ),
karuru_dn_items as (
                    select distinct --date(created_at) as created_at,
                    coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                    country_code,
                    id,
                    code,
                    dn.status,
                    dn.outlet_id,
                    case 
                      when dn.territory_id in ('Kano-Sabongari', 'Kano-Zoo') then 'Gandu'
                      when dn.territory_id in ('Abuja-Bwari', 'Nassarawa-Karu') then 'Kubwa'
                    else dn.territory_id end as territory_id,  
                    sum(oi.total_delivered) as total_delivered
                    from karuru_dns dn, unnest(order_items) oi
                    where index = 1
                    AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
                    and oi.status = 'ITEM_FULFILLED'
                    group by 1,2,3,4,5,6,7
                    )
select *
from karuru_dn_items