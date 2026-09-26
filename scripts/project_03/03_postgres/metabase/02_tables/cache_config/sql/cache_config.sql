with
cache_config_cte as (
	SELECT id, model, model_id, created_at, updated_at, strategy, config, state, invalidated_at, next_run_at, refresh_automatically
	FROM public.cache_config
	)
select *
from cache_config_cte