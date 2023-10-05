-------------------- Karuru -------------------
------------------- Dns -----------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by code order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) between '2022-02-01' and '2023-09-16'
                --where date(created_at) between '2023-09-03' and '2023-09-06'
                where date(created_at) > '2023-07-23'
                and is_pre_karuru = false
                ),
dns_report as (
                select distinct date(created_at) as created_at,
                --territory_id,
                code,
                id,
                --sale_order_id,
                status,
                --payment_request_id,
                from karuru_dns dn
                where index = 1 
                --and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                --and dn.country_code = 'KE'
                --and dn.status in ('DELIVERED', 'PAID')
                )
select *
from dns_report 
where status = 'UNRECOGNIZED'
order by 1 desc