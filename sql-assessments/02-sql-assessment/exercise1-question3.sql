--
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