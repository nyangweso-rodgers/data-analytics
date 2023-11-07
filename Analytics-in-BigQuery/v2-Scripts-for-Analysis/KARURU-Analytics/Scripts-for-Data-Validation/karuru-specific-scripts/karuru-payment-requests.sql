------------------------- Karuru ----------------------
------------------------ Payment Requests ---------------------
with
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
                    s.transaction_reference,
                    --s.channel,
                    s.amount as settlement_amount
                    from karuru_payment_requests pr, unnest(settlement) s
                    where index = 1
                    )
select *
from payment_request