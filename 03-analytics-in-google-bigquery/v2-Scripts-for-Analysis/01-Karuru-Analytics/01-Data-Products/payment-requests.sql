
------------------------ PRs ---------------------
with
payment_requests as (
                SELECT *,
                row_number()over(partition by id order by last_modified desc) as index
                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                WHERE DATE(created_at) >= "2023-01-01" 
                --where DATE(created_at) <= '2024-01-07'
                --where date(created_at) <= date_sub(date_trunc(current_date, month),interval 1 day)
                ),
payment_requests_report as (
                              select distinct --created_at,
                              DATE(created_at) as created_at,
                              country_code,
                              id,
                              payment_reference,
                              pr.status,
                              purpose,
                              --payment_type,
                              --pr.amount,
                              --payment_reference,
                              from payment_requests pr
                              where index = 1
                              --and pr.status in ('CASH_COLLECTED', 'PROCESSING', 'QUEUED')
                              ),
payment_requests_with_settlement as (
                                    select distinct --created_at,
                                    DATE(created_at) as created_at,
                                    
                                    country_code,
                                    id,
                                    payment_reference,
                                    pr.status,
                                    purpose, 
                                    s.status as settlement_status,
                                    s.transaction_reference,
                                    s.channel,
                                    s.settlement_type,
                                    --s.amount as settlement_amount
                                    from payment_requests pr, unnest(settlement) s
                                    where index = 1
                                    ),
payment_requests_with_settlement_report as (
                                            select prr.*,
                                            prws.status as settlement_status,
                                            prws.transaction_reference,
                                            prws.channel as settlement_channel,
                                            prws.settlement_type
                                            from payment_requests_report prr
                                            left join payment_requests_with_settlement prws on prr.id = prws.id
                                            )
select *
from payment_requests_with_settlement
--where payment_reference = '0FWK97MDPQNST'
where payment_reference = '0G1SC53CPNPR5'
order by created_at desc, id
