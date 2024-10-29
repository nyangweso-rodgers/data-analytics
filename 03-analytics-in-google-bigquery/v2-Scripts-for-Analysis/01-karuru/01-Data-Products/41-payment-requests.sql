
------------------------ payment requests ---------------------
with
payment_requests as (
                SELECT *,
                row_number()over(partition by id order by last_modified desc) as index
                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                WHERE DATE(created_at) >= "2021-01-01" 
                --where DATE(created_at) <= '2024-01-07'
                --where DATE(created_at) between '2024-01-01' and '2024-10-30' 
                --where date(created_at) >= date_sub(date_trunc(current_date, month),interval 1 week)
                and country_code = 'KE'
                --and payment_reference = '0HF8VVFEV7G6A'
                --and payment_reference = '0GATQ0A048TZY'
                and purpose = 'CREDIT_REPAYMENT'
                --and id = '0GD58XQQHGK65'
                --and payment_reference = '0GAJT0FRJ771W'
                --and payment_reference = '0GAJT0FRJ771W'
                /*and id in ("0GE0SZ0N3W2CA",
"0GDRFWNP64C3K",
"0GDRF7XKJ4D3Z",
"0GDRF3DNE485Y",
"0GDREY5A2407V",
"0GDREND6J4DFW",
"0GDDVR3MDGRV8",
"0GDC3WM35GV5T",
"0GD58XQQHGK65",
"0GD58X331GR4C",
"0GD4X07MNHB5A",
"0GATQ0A048TZY")*/
--and id = '0G9MV0GHRK9SR'
                ),
pr_cte as (
            select distinct pr.created_at,
            pr.bq_upload_time,

            pr.country_code,

            pr.id,
            pr.payment_reference,
            pr.status,
            pr.purpose,
            --payment_type,
            --pr.amount,
            --payment_reference,
            from payment_requests pr 
            where index = 1
            --and pr.status in ('CASH_COLLECTED', 'PROCESSING', 'QUEUED')
            ),
pr_settlements_cte as (
                        select distinct id, 
                        s.status as settlement_status,
                        s.transaction_reference,
                        s.channel,
                        s.settlement_type,
                        s.amount as settlement_amount
                        from payment_requests pr, unnest(settlement) s
                        where index = 1
                        ),
pr_with_settlement_cte as (
                          select distinct pr.created_at,
                          pr.bq_upload_time,

                          pr.country_code,
                          pr.id as payment_request_id,
                          pr.status as payment_request_status,

                          prs.settlement_status,
                          prs.transaction_reference,
                          prs.channel as settlement_channel,
                          prs.settlement_type,
                          settlement_amount
                          from pr_cte pr
                          left join pr_settlements_cte prs on pr.id = prs.id
                          )
select *
--max(created_at) as max_created_at, max(bq_upload_time) as max_bq_upload_time
--count(distinct id) as pr_count
--from pr_with_settlement_cte
from pr_cte
--order by created_at desc, id