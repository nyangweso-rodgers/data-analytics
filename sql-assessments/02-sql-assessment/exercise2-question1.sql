---- Monthly cohort retention
with
agents_cte as (
                select distinct id,
                date(account_opened_at) as account_opened_at,
                from agents
                where domain = 'BUSINESS'
                ),
agents_monthly_transactions as (
                                select distinct agent_id,
                                date_trunc(date(trxn_date), month) as trxn_month,
                                transaction_id
                                from transactions
                                where agent_id in (select distinct agent_id from agents_cte)
                                ),
agents_with_their_first_trxn_month as (
                                        select distinct agent_id,
                                        min(trxn_month) as first_trxn_month
                                        from agents_monthly_transactions
                                        ),
-- find the size of each cohort by by counting the number of unique stalls that show up for the first time in a month
monthly_cohort_size as (
                        select extract(year from first_trxn_month) as joining_year,
                        extract(month from first_trxn_month) as joining_month, 
                        count(1) as cohort_size
                        from agents_with_their_first_trxn_month
                        ),
monthly_agent_activities as (
                            select distinct agents_monthly_transactions.agent_id
                            date_diff(agents_monthly_transactions.trxn_month, agents_with_their_first_trxn_month.first_trxn_month, month) as month_number,
                            from agents_monthly_transactions
                            left join agents_with_their_first_trxn_month on agents_with_their_first_trxn_month.agent_id = agents_monthly_transactions.agent_id
                            ),

cohort_retention_table as (
                            select extract(year from agents_with_their_first_trxn_month.first_trxn_month) as cohort_joining_year,
                            extract(month from agents_with_their_first_trxn_month.first_trxn_month) as cohort_joining_month,
                            monthly_agent_activities.month_number,
                            count(1) as count_of_agents
                            from monthly_agent_activities
                            left join agents_with_their_first_trxn_month on agents_with_their_first_trxn_month.agent_id = monthly_agent_activities.agent_id
                            group by 1,2,3
                            ),
cohort_retention_table_summary as (
                                    select distinct cohort_retention_table.cohort_joining_year,
                                    cohort_retention_table.cohort_joining_month,
                                    cohort_retention_table.month_number,
                                    monthly_cohort_size.cohort_size as cohort_size,
                                    cohort_retention_table.count_of_agents,
                                    cast(cohort_retention_table.count_of_agents as float64) /monthly_cohort_size.cohort_size as agents_cohort_percentage_retention,
                                    from cohort_retention_table
                                    left join monthly_cohort_size on cohort_retention_table.cohort_joining_year = monthly_cohort_size.joining_year and cohort_retention_table.cohort_joining_month = monthly_cohort_size.joining_month
                                    )
select * from cohort_retention_table_summary