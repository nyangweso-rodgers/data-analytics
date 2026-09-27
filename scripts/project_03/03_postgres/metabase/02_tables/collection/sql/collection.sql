with
collection_cte as (
	SELECT id, "name", description, archived, "location", personal_owner_id, slug, "namespace", authority_level, entity_id, created_at, "type", is_sample, archive_operation_id, archived_directly, is_remote_synced
	FROM public.collection
	order by name
	)
select *
from collection_cte