with
regions_cte as (
	SELECT id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	--meta, 
	--old_region_id, 
	region_name, 
	country_id
	FROM public.regions
	)
select *
from regions_cte