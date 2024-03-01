------------------------- Karuru ----------------------
------------------------ PRs ---------------------
with
karuru_pr as (
                SELECT *,
                row_number()over(partition by id order by last_modified desc) as index
                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                --WHERE DATE(created_at) >= "2023-01-01" 
                where DATE(created_at) <= '2024-01-07'
                ),
pr_summary as (
            select distinct --created_at,
            DATE(created_at) as created_at,
            id,
            pr.status,
            --pr.amount,
            --payment_reference,
            from karuru_pr pr
            where index = 1
            --and pr.status in ('CASH_COLLECTED', 'PROCESSING', 'QUEUED')
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
where id in ('0EQZWXVYF85T7')