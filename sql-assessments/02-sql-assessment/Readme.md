# Question and Answer - Submission File

# Exercise 1, Question 1

- Number of **NEW AGENTS** per week split by which of them are coming from referrals vs which ones are not coming from referrals.

```sql
    with
    users_cte as (
                select distinct user_id,
                username
                from users
                ),
    agents_cte as (
                select distinct agent_id,
                user_id,
                date(created_on) as created_on
                from agents
                ),
    referees_cte as (
                    select distinct username
                    from referees
                    ),
    new_agents_per_day as (
                            select distinct agents_cte.agent_id,
                            case
                                when referees_cte.username is not null then 'Referred'
                            else 'Not Referred' end as referral_status,
                            min(agents_cte.created_on) as created_on
                            from agents_cte
                            left join users_cte on users_cte.user_id = agents_cte.user_id
                            left join referees_cte on users_cte.username = referees_cte.username
                            group by agents_cte.agent_id, referral_status
                            ),
    new_agents_per_week as (
                            select distinct date_trunc(created_on, week) as signup_week,
                            referral_status,
                            count(distinct agent_id) as new_agents_count
                            from new_agents_per_day
                            group by signup_week, referral_status
                            )
    select * from new_agents_per_week
    order by signup_week, referral_status
```

# Exercise 1, Question 2

- Number of agents with first time transactions per week (so, people who placed their first “any kind of” transaction) split by which of them are coming from referrals vs which ones are not coming from referrals.

```sql
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
```

# Exercise 1, Question 3

- Total cost of referrals per day and the Cost of Acquisition of that cost (Total Referrals Cost/Total New Referral Agents created)

  ```sql
  with
  referees_cte as (
                  select distinct username,
                  date(signup_date) as signup_date,
                  cost
                  from referees
                  ),
  daily_referal_costs as (
                          select distinct signup_date,
                          count(distinct username) as referrals_count,
                          sum(cost) as referral_cost,
                          sum(cost) / count(distinct username) as cost_of_acquisition
                          from referees_cte
                          group by signup_date
                          )
  select * from daily_referal_costs
  ```

# Exercise 2

- We want to create Retention Cohorts to evaluate how we retain users over time. The goal is to create cohorts starting from the agent’s First Transaction Month and check how they retain in the following months. For example, for all those customers who are New Transacting Agents in January 2023, how many of them retained on month +1 (so, February) and on month +2 (so, March) and on month +3 and so on (up until today). Build a SQL query to create a retention cohort starting on First Transaction Month for all our users since inception. Show the result only for Business Accounts. You have the following two tables:

  ```sql
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
  ```
