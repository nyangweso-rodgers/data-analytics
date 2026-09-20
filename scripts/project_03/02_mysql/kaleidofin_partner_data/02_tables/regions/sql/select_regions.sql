with
regions_cte as (
	SELECT id, created_at, updated_at, region_name, _exported_at
	FROM kaleidofin_partner_data.regions
	)
select count(*)
from regions_cte