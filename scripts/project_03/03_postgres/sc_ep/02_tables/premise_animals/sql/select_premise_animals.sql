with
premise_animals_cte as (
	SELECT id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	--meta, 
	premise_id, 
	animal_name, 
	"number"
	FROM public.premise_animals
	),
premises_cte as (
	SELECT meta, 
	account_id, 
	id, 
	created_at, 
	--updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	--old_premise_id, 
	premise_name, 
	customer_id, 
	premise_type_id, 
	premise_number, 
	substate_id, 
	town, 
	courier_location_id,
	is_validated
	FROM public.premises
	),
premise_animals_mashuop_cte as (
	select distinct premise_animals_cte.*,
	premises_cte.premise_name,
	premises_cte.customer_id,
	premises_cte.town
	from premise_animals_cte
	left join premises_cte on premises_cte.id = premise_animals_cte.premise_id
)	
select *
--distinct animal_name
from premise_animals_mashuop_cte