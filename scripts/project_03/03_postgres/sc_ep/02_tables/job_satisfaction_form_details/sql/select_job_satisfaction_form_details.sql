with 
job_satisfaction_form_details_cte as (
SELECT meta, id, created_at, updated_at, 
created_by, updated_by, is_active, 
job_satisfaction_form_id, all_items_received, 
payg_contract_number, tank_stand_constructed, tank_procured, beds_made, 
equipment_on_site, has_workers, one_trained_person, water_in_tank, has_enough_pegs, 
head_unit_installed, layflat_fittings_installed, drip_line_fittings_installed, system_flushed, 
system_tested, trained_irrigation_scheduling, trained_filter_cleaning, 
trained_system_flushing, trained_installing_valves, trained_tightening_drip_lines, 
controller_icon_working, ground_or_roof, roof_assessment_done, ground_assessment_done, 
electric_cables_connected, hdpe_fittings_connected, rope_tied, hdep_pipe_installed, 
pump_test_done, water_quality, seconds_to_fill_bucket, 
sprinkler_installed, trained_keeping_panels_clean, 
trained_connect_solar_panels, trained_connect_rainmaker, 
trained_connect_hdpe, trained_daily_operations, 
trained_connect_sprinkler, trained_manual_solar_tracking, trained_check_battery_voltage, 
trained_interpret_controller_icons, trained_how_to_measure_seconds, picture_client, national_id_pic, 
agent_signature, customer_signature, payg_contract, payg_contract1, payg_contract2, 
payg_contract3, picture_well, picture_battery_controller_location, picture_solar_panel_location, 
warranty_card_pic, picture_tank_filter, picture_irrigation, 
has_water, water_source, when_well_was_dug, 
monthly_pumping_cost, picture_of_water_source, 
water_source_depth, water_distance_from_source_to_point_of_use, 
has_signed_carbon_form, payg_contract4, contract_recieved, 
national_id_back_side, has_water_abstraction_permit, had_gov_authority_water_abstraction_interactions,
owned_diesel_petrol_pump, previous_pump_type, previous_fuel_monthly_spend
FROM public.job_satisfaction_form_details
)
select --*
count(*)
--distinct min(when_well_was_dug)
from job_satisfaction_form_details_cte
order by 1