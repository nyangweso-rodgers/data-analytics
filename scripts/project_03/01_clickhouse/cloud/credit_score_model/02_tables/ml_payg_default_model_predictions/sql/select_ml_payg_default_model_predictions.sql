WITH
ml_payg_default_model_predictions_cte as (
    SELECT * 
    FROM credit_score_model.ml_payg_default_model_predictions
    ),
agg_cte as (
    select distinct score_month,
    count(*)
    from ml_payg_default_model_predictions_cte
    GROUP BY 1
    ORDER BY 1
)
select *
--from ml_payg_default_model_predictions_cte
from agg_cte
LIMIT 1000