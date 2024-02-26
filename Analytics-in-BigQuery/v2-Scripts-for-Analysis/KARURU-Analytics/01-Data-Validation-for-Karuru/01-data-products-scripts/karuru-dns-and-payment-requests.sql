--------------------- Karuru ---------------
------------------ DNs & Payment Requests  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by code order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-05'
                and is_pre_karuru = false
                ),
dns_items as (
              select distinct date(created_at) as created_at,
              country_code,
              id,
              code,
              dn.status as dn_status,
              dn.payment_request_id,
              sum(total_delivered) as total_delivered
              from karuru_dns dn, unnest(order_items) dni
              where index = 1
              --and country_code = 'TZ'
              AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
              and dni.status = 'ITEM_FULFILLED'
              and dn.country_code = 'KE'
              --and dn.id in ('0DZZE5Y4E6G67')
              --and payment_request_id = '0E4N6ASR49R96'
              group by 1,2,3,4,5,6
              ),
karuru_payment_requests as (
                            SELECT *,
                            row_number()over(partition by id order by last_modified desc) as index
                            FROM `kyosk-prod.karuru_reports.payment_requests` pr
                            WHERE DATE(created_at) >= "2023-01-01" 
                            ),
payment_request as (
                    select distinct DATE(created_at) as created_at,
                    id,
                    payment_reference,
                    --s.transaction_reference,
                    --s.channel,
                    pr.status as pr_status,
                    pr.amount as payment_request_amount,
                    s.status as settlement_status,
                    sum(s.amount) as settlement_amount
                    from karuru_payment_requests pr, unnest(settlement) s
                    where index = 1
                    group by 1,2,3,4,5,6
                    --and id = '0E4N6ASR49R96'
                    ),
dn_pr_mashup as (
                  select dn.*,
                  --pr.transaction_reference,
                  pr.id as pr_id,
                  pr.pr_status,
                  pr.payment_request_amount,
                  pr.settlement_status,
                  pr.settlement_amount,
                  dn.total_delivered = pr.settlement_amount as check_revenue
                  from dns_items dn
                  left join payment_request pr on dn.payment_request_id = pr.id
                  )
select * from dn_pr_mashup
where pr_id is not null and check_revenue = false
order by created_at desc