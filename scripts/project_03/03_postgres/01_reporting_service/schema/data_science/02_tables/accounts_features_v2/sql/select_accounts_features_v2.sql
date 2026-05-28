with
accounts_features_v2_cte as (
	SELECT customer_id, 
	account_id, 
	status, 
	_feature_generated_at, 
	_exported_at
	FROM data_science.accounts_features_v2
	)
select *
from accounts_features_v2_cte
where customer_id = '10002'