WITH
ml_customer_location_features_cte as (
    SELECT distinct customerId,
    country,
    region,
    county,
    subcounty,
    latitude,
    longitude,
    customerCreatedDate,
    _feature_generated_at
    FROM credit_score_model.ml_customer_location_features
    ) 
select *
from ml_customer_location_features
limit 1000