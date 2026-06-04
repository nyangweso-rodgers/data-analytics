with
substates_cte as (
	SELECT id, created_at, updated_at, state_id, substate_type, substate_name, _exported_at
	FROM kaleidofin_partner_data.substates
	)
select count(*)
from substates_cte