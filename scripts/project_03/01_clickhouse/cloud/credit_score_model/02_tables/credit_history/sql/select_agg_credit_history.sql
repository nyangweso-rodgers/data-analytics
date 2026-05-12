WITH
agg_credit_history_v1_cte as (
    SELECT * 
    FROM credit_score_model.agg_credit_history_v1
    )
select --*
--COUNT(*), COUNT(distinct customerId), count(distinct accountId)
from agg_credit_history_v1_cte
LIMIT 1000