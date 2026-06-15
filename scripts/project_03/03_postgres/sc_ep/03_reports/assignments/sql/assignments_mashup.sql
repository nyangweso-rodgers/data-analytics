with
assignments_cte as (
	SELECT --meta, 
	id, 
	created_at, 
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
	--ticket_number, 
	--"number", 
	--"comment"
	FROM public.assignments
	),
schedules_cte as (
	SELECT --meta, 
	id, 
	--created_at, 
	--updated_at, 
	--created_by, 
	--updated_by, 
	--is_active, 
	assignment_id, 
	--scheduled_by, 
	scheduled_date, 
	completed_date
	FROM public.schedules
),
jsf_cte as (
	SELECT --meta, 
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
	--casual_pay, 
	--costings, 
	outcome_reason, 
	--jsf_start_time, 
	--jsf_end_time, 
	engineer_recommendation, 
	--device_image, 
	product_type, 
	--"comment", 
	approval_date, 
	approved_by, 
	submission_date, 
	submitted_by
	FROM public.job_satisfaction_form
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
	--courier_location_id,
	--is_validated
	FROM public.premises
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
--	reason_for_buying, 
	--has_water_abstraction_permit, 
	--landmark_name, 
	--landmark_gps, 
	--customer_alias, 
	district, 
	county, 
	subcounty, 
	--parish, 
	village
	FROM public.premise_details
	),
assignments_mashup_cte as (
    select distinct 
    county,
    subcounty,
    premises_cte.town,
    district,
    village,
    premises_cte.customer_id,
    assignments_cte.account_id as account_id,
    assignments_cte.id as assignment_id,
    assignments_cte.assignment_type,
    assignments_cte.assignment_date,
    schedules_cte.id as schedule_id,
    schedules_cte.scheduled_date,
    schedules_cte.completed_date as schedule_completed_date,
    jsf_cte.id as jsf_id,
	jsf_cte.created_at as jsf_created_at,
    jsf_cte.completed_date as jsf_completed_date,
    jsf_cte.submission_date as jsf_submission_date,
    jsf_cte.jsf_status,
    jsf_cte.jsf_type,
    jsf_cte.outcome_reason,
    jsf_cte.engineer_recommendation,
    assignments_cte.engineer_id
    from assignments_cte
    left join schedules_cte on assignments_cte.id = schedules_cte.assignment_id
    left join jsf_cte on schedules_cte.id = jsf_cte.schedule_id
    left join premises_cte on assignments_cte.premises_id = premises_cte.id
    left join premise_details_cte on premises_cte.id = premise_details_cte.premise_id
	order by customer_id, account_id, assignment_date
    )
select *
from assignments_mashup_cte
where account_id = '23315'