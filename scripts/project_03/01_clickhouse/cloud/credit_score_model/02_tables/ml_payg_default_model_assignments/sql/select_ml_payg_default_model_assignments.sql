WITH
ml_payg_default_model_assignments as (
    select *
    from credit_score_model.ml_payg_default_model_assignments
    ),
agg_cte as (
    select 
    count(*)
    from ml_payg_default_model_assignments
    )
select *
--distinct score_month
--from ml_payg_default_model_assignments
from agg_cte
limit 1000