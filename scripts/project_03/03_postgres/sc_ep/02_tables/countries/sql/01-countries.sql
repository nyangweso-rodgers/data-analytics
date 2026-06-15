with
countries_cte as (
	SELECT --meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	--old_country_id, 
	--iso_code, 
	country_name
	--timezone
	FROM public.countries
	)
select * from countries_cte