with
tickets_cte as (
	SELECT id, group_id, priority_id, state_id, organization_id, "number", title, 
	owner_id, customer_id, note, first_response_at, first_response_escalation_at, first_response_in_min, 
	first_response_diff_in_min, close_at, close_escalation_at, close_in_min, close_diff_in_min, update_escalation_at, 
	update_in_min, update_diff_in_min, last_close_at, last_contact_at, last_contact_agent_at, last_contact_customer_at, last_owner_update_at, create_article_type_id, create_article_sender_id, article_count, escalation_at, pending_time, "type", time_unit, preferences, updated_by_id, created_by_id, created_at, updated_at, checklist_id, account, date_received, deviceid, component_cost, cost_repair_and_logistics, product_type_split, product_type, testing_and_repair_resolution, failure_cause, inquiry_type, it_tittle_support
	FROM public.tickets
	)
select 
count(*)
from tickets_cte