-------------------- Karuru -------------------
------------------- Dns vs. Payment Request -----------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by code order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) between '2022-02-01' and '2023-09-16'
                --where date(created_at) between '2023-09-03' and '2023-09-06'
                where date(created_at) <= '2023-09-23'
                and is_pre_karuru = false
                ),
dns_report as (
                select distinct date(created_at) as created_at,
                territory_id,
                code,
                id,
                status,
                payment_request_id,
                from karuru_dns dn
                where index = 1 
                and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                --and dn.country_code = 'KE'
                and dn.status in ('DELIVERED', 'PAID')
                --and code = 'DN-RUAI-FVQ0'
                ),
karuru_payment_request as (
                            SELECT *,
                            row_number()over(partition by id order by last_modified desc) as index
                            FROM `kyosk-prod.karuru_reports.payment_requests` pr
                            WHERE DATE(created_at) >= "2023-01-01" 
                            ),
payment_request as (
                    select distinct 
                    id,
                    payment_reference,
                    amount
                    --s.transaction_reference,
                    --s.channel,
                    --s.amount as settlement_amount
                    from karuru_payment_request pr--, unnest(settlement) s
                    where index = 1
                    --and id = '0DJCEG9A8C2NA'
                    --and id = '0DP72Y9MVJA0V'
                    )
select dn.*, pr.payment_reference
from dns_report dn
left join payment_request pr on dn.payment_request_id = pr.id
where pr.id is null