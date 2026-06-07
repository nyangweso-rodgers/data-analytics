INSERT INTO public.assignments (
    id,
    meta,
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
    number,
    comment
) VALUES 
(
	, --id
	'{}', --meta
	CURRENT_TIMESTAMP AT TIME ZONE 'UTC', -- created_at
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC', --updated_at
    895, -- created_by
    895, -- updated_by
    TRUE, -- is_active
    CURRENT_TIMESTAMP,
    -- premises_id (populate from premises.id table)
    895 -- engineer_id
    'INSTALLATION', -- assignment_type, 
    895, -- assigned_by
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC', -- assignment_date
    58821 -- account_id
    NULL, -- ticket_id
    NULL, -- ticket_number
    NULL -- number
    NULL -- comment
)