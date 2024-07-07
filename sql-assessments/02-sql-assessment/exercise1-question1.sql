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