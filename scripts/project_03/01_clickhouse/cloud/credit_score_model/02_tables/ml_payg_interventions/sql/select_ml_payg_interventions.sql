WITH
ml_payg_interventions_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by accountId ORDER BY scored_at desc) as rnk 
        FROM credit_score_model.ml_payg_interventions
        ) where rnk = 1
        and country = 'kenya'
    ) 
select --*
count(*), max(scored_at)
from ml_payg_interventions_cte
LIMIT 31 OFFSET 0;