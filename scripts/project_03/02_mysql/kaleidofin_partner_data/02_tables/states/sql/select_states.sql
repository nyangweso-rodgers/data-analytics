with
states_cte as (
	SELECT id, created_at, updated_at, state_type, state_name, region_id, _exported_at
	FROM kaleidofin_partner_data.states
	)
select count(*)
from states_cte