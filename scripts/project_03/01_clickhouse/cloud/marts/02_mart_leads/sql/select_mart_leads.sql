WITH
mart_leads_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by leadId ORDER BY _generated_at desc) as rnk 
        FROM marts.mart_leads
        --FROM test.test_marts_mart_leads -- TEST
    ) where rnk = 1
    and country = 'kenya'
    ),
agg_leads_cte as (
    select --*
    distinct leadsource, count(distinct leadId)
    from mart_leads_cte
    group by 1 ORDER BY 2 desc
    )
select
distinct referralType
from mart_leads_cte
LIMIT 31