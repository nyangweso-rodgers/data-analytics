--------------------- Karuru ---------------
------------------ DNs Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-05'
                --and is_pre_karuru = false
                ),
dn_items as (
              select distinct --date(created_at) as created_at,
              coalesce(date(delivery_date), date(updated_at)) as delivery_date,
              country_code,
              id,
              --code,
              --dn.status,
              dn.outlet_id,
              outlet.name as outlet_name,
              --outlet.phone_number,
              --dn.agent_name as market_developer,
              --oi.status as item_status,
              case 
                when dn.territory_id in ('Kano-Sabongari', 'Kano-Zoo') then 'Gandu'
                when dn.territory_id in ('Abuja-Bwari', 'Nassarawa-Karu') then 'Kubwa'
              else dn.territory_id end as territory_id,
              --oi.product_bundle_id,  
              sum(oi.total_delivered) as total_delivered
              from karuru_dns dn, unnest(order_items) oi
              where index = 1
              AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
              and oi.status = 'ITEM_FULFILLED'
              group by 1,2,3,4,5,6
              ),
dns_with_last_details as (
                          select distinct 
                          last_value(territory_id)over(partition by outlet_id order by delivery_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as territory_id,
                          outlet_id,
                          last_value(outlet_name)over(partition by outlet_id order by delivery_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as outlet_name, 
                          --last_value(phone_number)over(partition by outlet_id order by delivery_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as phone_number
                          from dn_items
                          ),
ug_and_ng_report as (
                      select distinct country_code,
                      territory_id, 
                      count(distinct id) as count_of_dns, 
                      count(distinct outlet_id) as count_of_outlets, 
                      sum(total_delivered) as total_delivered
                      from dn_items
                      where delivery_date between '2024-02-01' and '2024-02-29'
                      and country_code in ('UG', 'NG')
                      group by 1,2
                      order by 1,2
                      ),
ke_and_tz_report as (
                      select distinct country_code,
                      territory_id, 
                      count(distinct id) as count_of_dns, 
                      count(distinct outlet_id) as count_of_outlets, 
                      sum(total_delivered) as total_delivered
                      from dn_items
                      where delivery_date between '2023-08-01' and '2023-08-31'
                      and country_code in ('KE', 'TZ')
                      group by 1,2
                      order by 1,2
                      )
select * from ke_and_tz_report
union all (select * from ug_and_ng_report)
order by 1,2