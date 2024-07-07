--
with
users_cte as (
            select distinct user_id,
            username
            from users
            ),
agents_cte as (
            select distinct agent_id,
            user_id
            from agents
            ),
referees_cte as (
                select distinct username
                from referees
                ),
transactions_cte as (
                     select distinct agent_id,
                     transaction_id,
                     date(transaction_date) as transaction_date
                     from card_transactions
                     union all (select distinct agent_id, transaction_id, date(transaction_date) as transaction_date  from withdrawal_transaction)
                     union all (select distinct agent_id, transaction_id, date(transaction_date) as transaction_date from deposit_transactions)
                     union all (select distinct agent_id, transaction_id, date(transaction_date) as transaction_date from bil_payment_transactions)
                     union all (select distinct agent_id, transaction_id, date(transaction_date) as transaction_date from airtime_transactions)
                     ),
agents_with_first_time_transactions as(
                                        select distinct agent_id,
                                        min(transaction_date) as first_transaction_date
                                        from transactions_cte
                                        group by agent_id
                                        ),
agents_with_first_transaction_per_week as (
                                            select distinct agent_id,
                                            date_trunc(transaction_date, week) as first_transaction_week,
                                            case
                                                when referees_cte.username is not null then 'Referred'
                                            else 'Not Referred' end as referral_status
                                            from agents_with_first_time_transactions
                                            left join agents_cte on agents_cte.agent_id = agents_with_first_time_transactions.agent_id
                                            left join users_cte on users_cte.user_id = agents_cte.user_id
                                            left join referees_cte on referees_cte.username = users_cte.username
                                            )
select distinct first_transaction_week, referral_status,
count(distinct agent_id) as agents_count
from agents_with_first_transaction_per_week
group by first_transaction_week, referral_status