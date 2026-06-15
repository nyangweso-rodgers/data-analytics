with
states_cte as (
	SELECT --meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	--old_state_id, 
	state_type, 
	state_name, 
	country_id, 
	region_id
	FROM public.states
	)
select distinct state_type 
from states_cte