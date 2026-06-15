with
premise_details_cte as (
	SELECT 
	--meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	premise_id, 
	--latitude, 
	--longitude, 
	--gps, 
	farm_management, 
	ownership_of_farm, 
	current_water_source, 
	--picture_of_water_source, 
	--has_water, 
	distance_to_water_source, 
	current_irrigation_method, 
	crops_to_be_grown, 
	did_soil_test, 
	electricity_on_farm, 
	water_tank_capacity_liter, 
	depth_of_water_source, 
	when_well_was_dug, 
	monthly_pumping_cost, 
	total_farm_size_acres, 
	reason_for_buying, 
	--has_water_abstraction_permit, 
	landmark_name, 
	landmark_gps, 
	customer_alias, 
	district, 
	county, 
	subcounty, 
	parish, 
	village
	FROM public.premise_details
	)
select *
--count(*)
from premise_details_cte