with
after_sale_form_details_cte as (
	SELECT meta, id, created_at, updated_at, created_by, 
	updated_by, is_active, job_satisfaction_form_id, product_type, 
	product_tag_id, issue_tag_id, resolution_tag_id, task_type, 
	battery_readings_before, battery_readings_after, tdh, tdh_after, flow_rate_before, flow_rate, ticket_id, has_water, water_source, when_well_was_dug, monthly_pumping_cost, picture_of_water_source, water_source_depth, water_distance_from_source_to_point_of_use, ticket_comment
	FROM public.after_sale_form_details
	)
select --*
distinct when_well_was_dug
--min(when_well_was_dug), max(when_well_was_dug)
from after_sale_form_details_cte
--where date(when_well_was_dug) = '1956-12-28'
order by 1