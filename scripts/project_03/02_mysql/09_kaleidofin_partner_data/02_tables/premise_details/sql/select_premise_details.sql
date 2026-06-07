with
premise_details_cte as (
	SELECT id, created_at, updated_at, premise_id, latitude, longitude, gps, _exported_at
	FROM kaleidofin_partner_data.premise_details
	)
select *
from premise_details_cte