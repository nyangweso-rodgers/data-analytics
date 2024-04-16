--------------------- Karuru ---------------
------------------ DNs with Settlement  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-01'
                and is_pre_karuru = false
                ),
dns_with_settlement as (
                        select distinct outlet_id,
                        --code,
                        --outlet.name as outlet_name,
                        --outlet.phone_number,
                        dn.payment_request_id,
                        s.channel,
                        transaction_reference
                        from karuru_dns dn, unnest(settlements) s
                        where index = 1
                        AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                        and country_code = 'UG'
                        and s.channel in ('MTN')
                        )
select * from dns_with_settlement