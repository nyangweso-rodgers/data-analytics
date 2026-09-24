with 
ml_customer_location_features_cte as (
	SELECT customerid, latitude, longitude, _feature_generated_at, 
	customer_created_date, 
	_exported_at
	FROM data_science.ml_customer_location_features
	)
select *
from ml_customer_location_features_cte 
where customerid ='101388'
limit 100