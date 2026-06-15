with
premise_types_cte as (
	SELECT meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	--old_premise_type_id, 
	premise_type_name
	FROM public.premise_types
	)