WITH
accounts_features_v2_cte as (
    SELECT * 
    FROM credit_score_model.accounts_features_v2
    )
select *
from accounts_features_v2_cte
where customerId = '10002'