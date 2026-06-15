with
jsf_cte as (
	SELECT 
	--meta, 
	id, 
	created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	jsf_status, 
	schedule_id, 
	jsf_type, 
	completed_date, 
	--device_id, 
	--device_status, 
	--meal_costings, 
	--accommodation_cost, 
	--casual_pay, 
	--costings, 
	outcome_reason, 
	--jsf_start_time, 
	--jsf_end_time, 
	engineer_recommendation, 
	--transport_costs, 
	--device_image, 
	product_type, 
	--"comment", 
	--meal_costings_narration, 
	--accommodation_narration, 
	--transport_narration, 
	--other_costs, 
	--other_costs_narration, 
	--labour_costs, 
	--labour_costs_narration, 
	approval_date, 
	--approved_by, 
	--meal_costings_attachment, 
	--accommodation_attachment, 
	--transport_attachment, 
	--other_costs_attachment, 
	--labour_costs_attachment, 
	submission_date
	--submitted_by
	FROM public.job_satisfaction_form
	),
schedules_cte as (
	SELECT --meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	is_active, 
	assignment_id, 
	scheduled_by, 
	scheduled_date, 
	completed_date
	FROM public.schedules
),
assignments_cte as (
	SELECT --meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	premises_id, 
	engineer_id, 
	assignment_type, 
	--assigned_by, 
	assignment_date, 
	account_id
	--ticket_id, 
	--ticket_number
	--"number", 
	--"comment"
	FROM public.assignments
	),
premises_cte as (
	SELECT --meta, 
	account_id, 
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
	town
	--courier_location_id
	--is_validated
	FROM public.premises
	),
premise_types_cte as (
	SELECT --meta, 
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
	--reason_for_buying, 
	--has_water_abstraction_permit, 
	--landmark_name, 
	--landmark_gps, 
	--customer_alias, 
	--district, 
	county, 
	subcounty,
	--parish, 
	village
	FROM public.premise_details
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
jsf_mashup_cte as (
	select distinct 
	countries_cte.country_name,
	regions_cte.region_name,
	premise_details_cte.county,
	premise_details_cte.subcounty,
	premises_cte.town,
	substates_cte.substate_type,
	substates_cte.substate_name,
	premise_details_cte.village,
	premises_cte.customer_id,
	assignments_cte.account_id,
	assignments_cte.premises_id,
	premises_cte.premise_name,
	premise_types_cte.premise_type_name,
	schedules_cte.assignment_id,
	date(assignments_cte.assignment_date) as assignment_date,
	assignments_cte.assignment_type,
	jsf_cte.schedule_id,
	date(schedules_cte.scheduled_date) as scheduled_date,
	date(schedules_cte.completed_date) as schedules_completed_date,
	jsf_cte.id as jsf_id, 
	date(jsf_cte.created_at) as jsf_created_at,
	date(jsf_cte.approval_date) as jsf_approval_date,
	date(jsf_cte.submission_date) as jsf_submission_date,
	date(jsf_cte.completed_date) as jsf_completed_date,
	jsf_cte.jsf_type,
	jsf_cte.jsf_status,
	--jsf_cte.device_id,
	--jsf_cte.device_status,
	--jsf_cte.product_type,
	assignments_cte.engineer_id,
	jsf_cte.engineer_recommendation,
	jsf_cte.outcome_reason
	from jsf_cte
	left join schedules_cte on schedules_cte.id = jsf_cte.schedule_id
	left join assignments_cte on assignments_cte.id = schedules_cte.assignment_id
	left join premises_cte on premises_cte.id = assignments_cte.premises_id
	left join premise_types_cte on premise_types_cte.id = premises_cte.premise_type_id
	left join premise_details_cte on premise_details_cte.premise_id = premises_cte.id
	left join substates_cte on substates_cte.id = premises_cte.substate_id
	left join states_cte on states_cte.id = substates_cte.state_id
	left join countries_cte on countries_cte.id = states_cte.country_id
	left join regions_cte on regions_cte.id = states_cte.region_id
	order by country_name, customer_id, account_id, jsf_created_at
	)
select --*
distinct  jsf_type, jsf_status, engineer_recommendation, max(jsf_created_at)
from jsf_mashup_cte
--where country_name = 'Kenya'
--where country_name = 'Uganda'
--where outcome_reason in ('Self_Installation')
--and engineer_recommendation not in ('Resolved', 'Not resolved', 'Repossessed', 'Not repossessed')
--and jsf_completed_date between '2024-11-01' and '2025-02-14'
--where premises_id in  ('552bbce6-94e3-415f-8736-31e1adbf9655','6eebed4e-f522-4a42-939f-53399ed96ff8','4a837147-cf0b-4ba2-80af-79fb205304bc')
--and engineer_id = '895'
--where customer_id in ('4816')
--order by customer_id, account_id, assignment_date asc
GROUP BY 1,2,3
order by jsf_type, jsf_status, engineer_recommendation