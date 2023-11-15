------------------------- Karuru ----------------------
------------------------ PR ---------------------
with
karuru_pr as (
                SELECT *,
                row_number()over(partition by id order by last_modified desc) as index
                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                WHERE DATE(created_at) >= "2023-01-01" 
                --where DATE(created_at) = '2023-11-15'
                ),
pr_summary as (
            select distinct --created_at,
            DATE(created_at) as created_at,
            id,
            pr.status,
            pr.amount,
            --payment_reference,
            from karuru_pr pr
            where index = 1
            ),
pr_with_settlement as (
                        select distinct --created_at,
                        DATE(created_at) as created_at,
                        id,
                        --s.status,
                        --payment_reference,
                        --s.transaction_reference,
                        --s.channel,
                        --s.amount as settlement_amount
                        from karuru_pr pr, unnest(settlement) s
                        where index = 1
                        )
select *
from pr_summary
--where id = '0DK8SF0P96110'
where status = 'FAILED'