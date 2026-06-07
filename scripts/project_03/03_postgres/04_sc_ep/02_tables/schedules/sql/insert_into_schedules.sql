INSERT INTO public.schedules (
	meta, 
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
) values 
(
	-- meta
	CURRENT_TIMESTAMP AT TIME ZONE 'UTC' -- created_at (current datetime)
	CURRENT_TIMESTAMP AT TIME ZONE 'UTC' -- updated_at (current datetime)
	895, -- created_by
	895, -- updated_by
	TRUE, -- is_active
	-- assignment_id (from assignment table)
	895 -- scheduled_by
	CURRENT_TIMESTAMP AT TIME ZONE 'UTC', -- scheduled_date
	CURRENT_TIMESTAMP AT TIME ZONE 'UTC' -- completed_date
	
)