with
premises_cte as (
	SELECT --meta, 
	--unnest(account_id) as account_id, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	--old_premise_id, 
	premise_name, 
	customer_id, 
	premise_type_id, 
	premise_number, 
	substate_id, 
	town, 
	courier_location_id
	--is_validated
	FROM public.premises
	),
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
	),
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
	),
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
	),
regions_cte as (
	SELECT id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	--meta, 
	--old_region_id, 
	region_name, 
	country_id
	FROM public.regions
	),
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
	--farm_management, 
	--ownership_of_farm, 
	--current_water_source, 
	--picture_of_water_source, 
	--has_water, 
	--distance_to_water_source, 
	--current_irrigation_method, 
	--crops_to_be_grown, 
	--did_soil_test, 
	--electricity_on_farm, 
	--water_tank_capacity_liter, 
	--depth_of_water_source, 
	--when_well_was_dug, 
	--monthly_pumping_cost, 
	--total_farm_size_acres, 
	--reason_for_buying
	--has_water_abstraction_permit, 
	--landmark_name, 
	--landmark_gps, 
	--customer_alias, 
	district, 
	--county -- all null
	subcounty, 
	--parish, 
	village
	FROM public.premise_details
	),
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
	),
assignments_cte as (
	SELECT meta, 
	id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	premises_id, 
	engineer_id, 
	assignment_type, 
	assigned_by, 
	assignment_date, 
	account_id, 
	ticket_id, 
	ticket_number, 
	"number", 
	"comment"
	FROM public.assignments
	),
schedules_cte as (
	SELECT meta, 
	id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	assignment_id, 
	scheduled_by, 
	scheduled_date, 
	completed_date
	FROM public.schedules
),
jsf_cte as (
	SELECT --meta, 
	id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	jsf_status, 
	schedule_id, 
	jsf_type, 
	completed_date, 
	device_id, 
	device_status, 
	casual_pay, 
	costings, 
	outcome_reason, 
	jsf_start_time, 
	jsf_end_time, 
	engineer_recommendation, 
	device_image, 
	product_type, 
	"comment", 
	approval_date, 
	approved_by, 
	submission_date, 
	submitted_by
	FROM public.job_satisfaction_form
	),
premises_mashup_cte as (
	select distinct countries_cte.country_name,
	regions_cte.region_name,
	village,
	states_cte.state_type,
	states_cte.state_name,
	premises_cte.id as premise_id,
	premises_cte.premise_number,
	premises_cte.town,
	substates_cte.substate_type,
	substates_cte.substate_name,
	premise_types_cte.premise_type_name,
	premise_details_cte.subcounty,
	premise_details_cte.district,
	premises_cte.customer_id,
	--premises_cte.premise_name
	assignments_cte.id as assignment_id,
	assignments_cte.created_at as assignment_created_at,
	schedules_cte.id as schedule_id,
	schedules_cte.created_at as schedule_created_at,
	jsf_cte.id as jsf_id,
	jsf_cte.created_at as jsf_created_at,
	jsf_cte.engineer_recommendation as jsf_engineer_recommendation,
	jsf_cte.outcome_reason as jsf_outcome_reason
	from premises_cte
	left join substates_cte on substates_cte.id = premises_cte.substate_id
	left join states_cte on states_cte.id = substates_cte.state_id 
	left join countries_cte on countries_cte.id = states_cte.country_id 
	left join premise_details_cte on premise_details_cte.premise_id = premises_cte.id
	left join premise_types_cte on premise_types_cte.id = premises_cte.premise_type_id
	left join regions_cte on regions_cte.id = states_cte.region_id
	left join assignments_cte on assignments_cte.premises_id = premises_cte.id
	left join schedules_cte on schedules_cte.assignment_id = assignments_cte.id
	left join jsf_cte on jsf_cte.schedule_id = schedules_cte.id
	order by customer_id
	)
select --*
distinct country_name, subcounty, village
from premises_mashup_cte
--where account_id in ('58821')
--where account_id in ('49680') -- successfully updated
--where account_id in ('23315')
--limit 20
--where premise_number = '17879'
--where customer_id = '4816'
--where customer_id in ('72125') -- requested to be updated to Self_installed, but currently mapped to Ug
--where customer_id = '46062'
--order by customer_id, premise_id, assignment_created_at asc
--where country_name = 'Kenya'
where customer_id = '65676'