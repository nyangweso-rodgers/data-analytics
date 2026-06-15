with
substates_cte as (
	SELECT --meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	--old_substate_id, 
	state_id, 
	substate_type, 
	substate_name
	FROM public.substates
	)
select distinct substate_type
from substates_cte