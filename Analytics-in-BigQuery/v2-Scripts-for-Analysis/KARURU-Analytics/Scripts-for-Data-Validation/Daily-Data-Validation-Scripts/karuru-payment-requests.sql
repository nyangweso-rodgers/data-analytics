------------------------- Karuru ----------------------
------------------------ Payment Requests ---------------------
with
payment_request_with_index as (
                                SELECT *,
                                row_number()over(partition by id order by last_modified desc) as index
                                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                                WHERE DATE(created_at) >= "2023-01-01" 
                                ),
payment_request as (
                    select distinct country_code,
                    DATE(created_at) as created_at,
                    id,
                    payment_reference,
                    s.transaction_reference,
                    s.channel,
                    s.amount as settlement_amount
                    from payment_request_with_index pr, unnest(settlement) s
                    where index = 1
                    --and country_code = 'KE'
                    --and id = '0DHA86C2Q6Q95'
                    --and id = '0DMMSAK3C24JZ'
                    )
select count(distinct id)
from payment_request
--order by created_at desc, id