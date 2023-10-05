-------------------------- DNs & Payment Requests --------------------
with
delivery_note_with_index as (
                              SELECT *,
                              row_number()over(partition by code order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                              where date(created_at) >= '2022-02-01'
                              --where date(created_at) between '2023-01-01' and '2023-09-04'
                              --and is_pre_karuru = false
                              ),
dns_summary as (
                select distinct date(created_at) as created_at,
                code,
                status,
                amount as delivery_note_amount,
                payment_request_id
                from delivery_note_with_index dn
                where index = 1
                --and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                and code in ('DN-LZRA-641X')
                ),
payment_request_with_index as (
                                SELECT *,
                                row_number()over(partition by id order by last_modified desc) as index
                                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                                WHERE DATE(created_at) >= "2023-08-01" 
                                ),
pr_summary as (
                select distinct id,
                s.amount as settlement_amount,
                pr.status as payment_request_status,
                from payment_request_with_index pr, unnest(settlement) s
                where index = 1
                and id in ('0DEH9HEMFYMBA')
                )
select dn.*,
pr.settlement_amount,
payment_request_status,
from dns_summary dn
left join pr_summary pr on dn.payment_request_id = pr.id